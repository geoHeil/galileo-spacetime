/*
Copyright (c) 2014, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.fs;

import java.awt.Polygon;
import java.awt.Rectangle;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.bmp.Bitmap;
import galileo.bmp.BitmapException;
import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityMap;
import galileo.bmp.GeoavailabilityQuery;
import galileo.comm.TemporalType;
import galileo.dataset.Block;
import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
import galileo.dataset.Point;
import galileo.dataset.SpatialHint;
import galileo.dataset.SpatialProperties;
import galileo.dataset.SpatialRange;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureSet;
import galileo.dataset.feature.FeatureType;
import galileo.dht.GroupInfo;
import galileo.dht.NetworkInfo;
import galileo.dht.NodeInfo;
import galileo.dht.PartitionException;
import galileo.dht.Partitioner;
import galileo.dht.StorageNode;
import galileo.dht.TemporalHierarchyPartitioner;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashTopologyException;
import galileo.dht.hash.TemporalHash;
import galileo.graph.FeaturePath;
import galileo.graph.MetadataGraph;
import galileo.graph.Path;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Operator;
import galileo.query.Query;
import galileo.serialization.SerializationException;
import galileo.serialization.Serializer;
import galileo.util.GeoHash;
import galileo.util.Math;
import galileo.util.Pair;

/**
 * Implements a {@link FileSystem} for Geospatial data. This file system manager
 * assumes that the information being stored has both space and time properties.
 * <p>
 * Relevant system properties include galileo.fs.GeospatialFileSystem.timeFormat
 * and galileo.fs.GeospatialFileSystem.geohashPrecision to modify how the
 * hierarchy is created.
 */
public class GeospatialFileSystem extends FileSystem {

	private static final Logger logger = Logger.getLogger("galileo");

	private static final String DEFAULT_TIME_FORMAT = "yyyy" + File.separator + "M" + File.separator + "d";
	private static final int DEFAULT_GEOHASH_PRECISION = 4;
	private static final int MIN_GRID_POINTS = 5000;
	private int numCores;

	private static final String pathStore = "metadata.paths";

	private NetworkInfo network;
	private Partitioner<Metadata> partitioner;
	private TemporalType temporalType;
	private int nodesPerGroup;
	/*
	 * Must be comma-separated name:type string where type is an int returned by
	 * FeatureType
	 */
	private List<Pair<String, FeatureType>> featureList;
	private SpatialHint spatialHint;
	private String storageRoot;

	private MetadataGraph metadataGraph;

	private PathJournal pathJournal;

	private SimpleDateFormat timeFormatter;
	private String timeFormat;
	private int geohashPrecision;
	private TemporalProperties latestTime;
	private TemporalProperties earliestTime;
	private String latestSpace;
	private String earliestSpace;
	private Set<String> geohashIndex;

	private static final String TEMPORAL_YEAR_FEATURE = "x__year__x";
	private static final String TEMPORAL_MONTH_FEATURE = "x__month__x";
	private static final String TEMPORAL_DAY_FEATURE = "x__day__x";
	private static final String TEMPORAL_HOUR_FEATURE = "x__hour__x";
	private static final String SPATIAL_FEATURE = "x__spatial__x";

	public GeospatialFileSystem(StorageNode sn, String storageDirectory, String name, int precision, int nodesPerGroup,
			int temporalType, NetworkInfo networkInfo, String featureList, SpatialHint sHint, boolean ignoreIfPresent)
			throws FileSystemException, IOException, SerializationException, PartitionException, HashException,
			HashTopologyException {
		super(storageDirectory, name, ignoreIfPresent);

		this.nodesPerGroup = nodesPerGroup;
		this.geohashIndex = new HashSet<>();
		if (featureList != null) {
			this.featureList = new ArrayList<>();
			for (String nameType : featureList.split(",")) {
				String[] pair = nameType.split(":");
				this.featureList
						.add(new Pair<String, FeatureType>(pair[0], FeatureType.fromInt(Integer.parseInt(pair[1]))));
			}
			this.featureList = Collections.unmodifiableList(this.featureList);
		}
		this.spatialHint = sHint;
		if (this.featureList != null && this.spatialHint == null)
			throw new IllegalArgumentException("Spatial hint is needed when feature list is provided");
		this.storageRoot = storageDirectory;
		this.temporalType = TemporalType.fromType(temporalType);
		this.numCores = Runtime.getRuntime().availableProcessors();

		if (nodesPerGroup <= 0) {
			this.network = new NetworkInfo();
			List<GroupInfo> groups = networkInfo.getGroups();
			TemporalHash th = new TemporalHash(this.temporalType);
			int maxGroups = th.maxValue().intValue();
			for (int i = 0; i < maxGroups; i++)
				this.network.addGroup(groups.get(i));
		} else {
			this.network = new NetworkInfo();
			GroupInfo groupInfo = null;
			List<NodeInfo> allNodes = networkInfo.getAllNodes();
			Collections.sort(allNodes);
			TemporalHash th = new TemporalHash(this.temporalType);
			int maxGroups = th.maxValue().intValue();
			for (int i = 0; i < allNodes.size(); i++) {
				if (this.network.getGroups().size() < maxGroups) {
					if (i % nodesPerGroup == 0) {
						groupInfo = new GroupInfo(String.valueOf(i / nodesPerGroup));
						groupInfo.addNode(allNodes.get(i));
						this.network.addGroup(groupInfo);
					} else {
						groupInfo.addNode(allNodes.get(i));
					}
				}
			}
		}

		/*
		 * TODO: Ask end user about the partitioning scheme. chronospatial or
		 * spatiotemporal. Accordingly, use TemporalHierarchyPartitioner or
		 * SpatialHierarchyPartitioner
		 **/
		this.partitioner = new TemporalHierarchyPartitioner(sn, this.network, this.temporalType.getType());

		this.timeFormat = System.getProperty("galileo.fs.GeospatialFileSystem.timeFormat", DEFAULT_TIME_FORMAT);
		int maxPrecision = GeoHash.MAX_PRECISION / 5;
		this.geohashPrecision = (precision < 0) ? DEFAULT_GEOHASH_PRECISION
				: (precision > maxPrecision) ? maxPrecision : precision;

		this.timeFormatter = new SimpleDateFormat();
		this.timeFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		this.timeFormatter.applyPattern(timeFormat);
		this.pathJournal = new PathJournal(this.storageDirectory + File.separator + pathStore);

		createMetadataGraph();
	}

	public JSONArray getFeaturesRepresentation() {
		JSONArray features = new JSONArray();
		if (this.featureList != null) {
			for (Pair<String, FeatureType> pair : this.featureList)
				features.put(pair.a + ":" + pair.b.name());
		}
		return features;
	}

	public List<String> getFeaturesList() {
		List<String> features = new ArrayList<String>();
		if (this.featureList != null) {
			for (Pair<String, FeatureType> pair : this.featureList)
				features.add(pair.a + ":" + pair.b.name());
		}
		return features;
	}

	public NetworkInfo getNetwork() {
		return this.network;
	}

	public Partitioner<Metadata> getPartitioner() {
		return this.partitioner;
	}

	public TemporalType getTemporalType() {
		return this.temporalType;
	}

	/**
	 * Initializes the Metadata Graph, either from a successful recovery from
	 * the PathJournal, or by scanning all the {@link Block}s on disk.
	 */
	private void createMetadataGraph() throws IOException {
		metadataGraph = new MetadataGraph();

		/* Recover the path index from the PathJournal */
		List<FeaturePath<String>> graphPaths = new ArrayList<>();
		boolean recoveryOk = pathJournal.recover(graphPaths);
		pathJournal.start();

		if (recoveryOk == true) {
			for (FeaturePath<String> path : graphPaths) {
				try {
					metadataGraph.addPath(path);
				} catch (Exception e) {
					logger.log(Level.WARNING, "Failed to add path", e);
					recoveryOk = false;
					break;
				}
			}
		}

		if (recoveryOk == false) {
			logger.log(Level.SEVERE, "Failed to recover path journal!");
			pathJournal.erase();
			pathJournal.start();
			fullRecovery();
		}
	}

	public synchronized JSONObject obtainState() {
		JSONObject state = new JSONObject();
		state.put("name", this.name);
		state.put("storageRoot", this.storageRoot);
		state.put("precision", this.geohashPrecision);
		state.put("nodesPerGroup", this.nodesPerGroup);
		state.put("geohashIndex", this.geohashIndex);
		StringBuffer features = new StringBuffer();
		if (this.featureList != null) {
			for (Pair<String, FeatureType> pair : this.featureList)
				features.append(pair.a + ":" + pair.b.toInt() + ",");
			features.setLength(features.length() - 1);
		}
		state.put("featureList", this.featureList != null ? features.toString() : JSONObject.NULL);
		JSONObject spHint = null;
		if (this.spatialHint != null) {
			spHint = new JSONObject();
			spHint.put("latHint", this.spatialHint.getLatitudeHint());
			spHint.put("lngHint", this.spatialHint.getLongitudeHint());
		}
		state.put("spatialHint", spHint == null ? JSONObject.NULL : spHint);
		state.put("temporalType", this.temporalType.getType());
		state.put("temporalString", this.temporalType.name());
		state.put("earliestTime", this.earliestTime != null ? this.earliestTime.getStart() : JSONObject.NULL);
		state.put("earliestSpace", this.earliestSpace != null ? this.earliestSpace : JSONObject.NULL);
		state.put("latestTime", this.latestTime != null ? this.latestTime.getEnd() : JSONObject.NULL);
		state.put("latestSpace", this.latestSpace != null ? this.latestSpace : JSONObject.NULL);
		state.put("readOnly", this.isReadOnly());
		return state;
	}

	public static GeospatialFileSystem restoreState(StorageNode storageNode, NetworkInfo networkInfo, JSONObject state)
			throws FileSystemException, IOException, SerializationException, PartitionException, HashException,
			HashTopologyException {
		String name = state.getString("name");
		String storageRoot = state.getString("storageRoot");
		int geohashPrecision = state.getInt("precision");
		int nodesPerGroup = state.getInt("nodesPerGroup");
		JSONArray geohashIndices = state.getJSONArray("geohashIndex");
		String featureList = null;
		if (state.get("featureList") != JSONObject.NULL)
			featureList = state.getString("featureList");
		int temporalType = state.getInt("temporalType");
		SpatialHint spHint = null;
		if (state.get("spatialHint") != JSONObject.NULL) {
			JSONObject spHintJSON = state.getJSONObject("spatialHint");
			spHint = new SpatialHint(spHintJSON.getString("latHint"), spHintJSON.getString("lngHint"));
		}
		GeospatialFileSystem gfs = new GeospatialFileSystem(storageNode, storageRoot, name, geohashPrecision,
				nodesPerGroup, temporalType, networkInfo, featureList, spHint, true);
		gfs.earliestTime = (state.get("earliestTime") != JSONObject.NULL)
				? new TemporalProperties(state.getLong("earliestTime")) : null;
		gfs.earliestSpace = (state.get("earliestSpace") != JSONObject.NULL) ? state.getString("earliestSpace") : null;
		gfs.latestTime = (state.get("latestTime") != JSONObject.NULL)
				? new TemporalProperties(state.getLong("latestTime")) : null;
		gfs.latestSpace = (state.get("latestSpace") != JSONObject.NULL) ? state.getString("latestSpace") : null;
		Set<String> geohashIndex = new HashSet<String>();
		for (int i = 0; i < geohashIndices.length(); i++)
			geohashIndex.add(geohashIndices.getString(i));
		gfs.geohashIndex = geohashIndex;
		return gfs;
	}

	public long getLatestTime() {
		if (this.latestTime != null)
			return this.latestTime.getEnd();
		return 0;
	}

	public long getEarliestTime() {
		if (this.earliestTime != null)
			return this.earliestTime.getStart();
		return 0;
	}

	public String getLatestSpace() {
		if (this.latestSpace != null)
			return this.latestSpace;
		return "";
	}

	public String getEarliestSpace() {
		if (this.earliestSpace != null)
			return this.earliestSpace;
		return "";
	}

	public int getGeohashPrecision() {
		return this.geohashPrecision;
	}

	private String getTemporalString(TemporalProperties tp) {
		if (tp == null)
			return "xxxx-xx-xx-xx";
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TemporalHash.TIMEZONE);
		c.setTimeInMillis(tp.getStart());
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int day = c.get(Calendar.DAY_OF_MONTH);
		int month = c.get(Calendar.MONTH) + 1;
		int year = c.get(Calendar.YEAR);
		switch (this.temporalType) {
		case HOUR_OF_DAY:
			return String.format("%d-%d-%d-%d", year, month, day, hour);
		case DAY_OF_MONTH:
			return String.format("%d-%d-%d-xx", year, month, day);
		case MONTH:
			return String.format("%d-%d-xx-xx", year, month);
		case YEAR:
			return String.format("%d-xx-xx-xx", year);
		}
		return String.format("%d-%d-%d-xx", year, month, day);
	}

	private String getSpatialString(SpatialProperties sp) {
		char[] hash = new char[this.geohashPrecision];
		Arrays.fill(hash, 'o');
		String geohash = new String(hash);
		if (sp == null)
			return geohash;

		if (sp.hasRange()) {
			geohash = GeoHash.encode(sp.getSpatialRange(), this.geohashPrecision);
		} else {
			geohash = GeoHash.encode(sp.getCoordinates(), this.geohashPrecision);
		}
		return geohash;
	}

	/**
	 * Creates a new block if one does not exist based on the name of the
	 * metadata or appends the bytes to an existing block in which case the
	 * metadata in the graph will not be updated.
	 */
	@Override
	public String storeBlock(Block block) throws FileSystemException, IOException {
		Metadata meta = block.getMetadata();
		String time = getTemporalString(meta.getTemporalProperties());
		String geohash = getSpatialString(meta.getSpatialProperties());
		String name = String.format("%s-%s", time, geohash);
		if (meta.getName() != null && meta.getName().trim() != "")
			name = meta.getName();
		String blockDirPath = this.storageDirectory + File.separator + getStorageDirectory(block);
		String blockPath = blockDirPath + File.separator + name + FileSystem.BLOCK_EXTENSION;
		String metadataPath = blockDirPath + File.separator + name + FileSystem.METADATA_EXTENSION;

		/* Ensure the storage directory is there. */
		File blockDirectory = new File(blockDirPath);
		if (!blockDirectory.exists()) {
			if (!blockDirectory.mkdirs()) {
				throw new IOException("Failed to create directory (" + blockDirPath + ") for block.");
			}
		}

		// Adding temporal and spatial features at the top
		// to the existing attributes
		FeatureSet newfs = new FeatureSet();
		String[] temporalFeature = time.split("-");
		newfs.put(new Feature(TEMPORAL_YEAR_FEATURE, temporalFeature[0]));
		newfs.put(new Feature(TEMPORAL_MONTH_FEATURE, temporalFeature[1]));
		newfs.put(new Feature(TEMPORAL_DAY_FEATURE, temporalFeature[2]));
		newfs.put(new Feature(TEMPORAL_HOUR_FEATURE, temporalFeature[3]));
		newfs.put(new Feature(SPATIAL_FEATURE, geohash));

		for (Feature feature : meta.getAttributes())
			newfs.put(feature);
		meta.setAttributes(newfs);

		Serializer.persist(block.getMetadata(), metadataPath);
		File gblock = new File(blockPath);
		boolean newLine = gblock.exists();
		if (!newLine)
			storeMetadata(meta, blockPath);
		/*
		 * TODO: Add an attribute to this class asking for block update strategy
		 * - whether to overwrite blocks, or append content. When it is append,
		 * ask for any delimiter to separate the existing data.
		 **/
		try (FileOutputStream blockData = new FileOutputStream(blockPath, true)) {
			if (newLine)
				blockData.write("\n".getBytes("UTF-8"));
			blockData.write(block.getData());
		} catch (Exception e) {
			throw new FileSystemException("Error storing block: " + e.getClass().getCanonicalName(), e);
		}

		if (latestTime == null || latestTime.getEnd() < meta.getTemporalProperties().getEnd()) {
			this.latestTime = meta.getTemporalProperties();
			this.latestSpace = geohash;
		}

		if (earliestTime == null || earliestTime.getStart() > meta.getTemporalProperties().getStart()) {
			this.earliestTime = meta.getTemporalProperties();
			this.earliestSpace = geohash;
		}

		this.geohashIndex.add(geohash.substring(0, Partitioner.SPATIAL_PRECISION));

		return blockPath;
	}

	public Block retrieveBlock(String blockPath) throws IOException, SerializationException {
		Metadata metadata = null;
		byte[] blockBytes = Files.readAllBytes(Paths.get(blockPath));
		String metadataPath = blockPath.replace(BLOCK_EXTENSION, METADATA_EXTENSION);
		File metadataFile = new File(metadataPath);
		if (metadataFile.exists())
			metadata = Serializer.deserialize(Metadata.class, Files.readAllBytes(Paths.get(metadataPath)));
		return new Block(this.name, metadata, blockBytes);
	}

	/**
	 * Given a {@link Block}, determine its storage directory on disk.
	 *
	 * @param block
	 *            The Block to inspect
	 *
	 * @return String representation of the directory on disk this Block should
	 *         be stored in.
	 */
	private String getStorageDirectory(Block block) {
		String directory = "";

		Metadata meta = block.getMetadata();
		directory = getTemporalDirectoryStructure(meta.getTemporalProperties()) + File.separator;

		Coordinates coords = null;
		SpatialProperties spatialProps = meta.getSpatialProperties();
		if (spatialProps.hasRange()) {
			coords = spatialProps.getSpatialRange().getCenterPoint();
		} else {
			coords = spatialProps.getCoordinates();
		}
		directory += GeoHash.encode(coords, geohashPrecision);

		return directory;
	}

	private String getTemporalDirectoryStructure(TemporalProperties tp) {
		return timeFormatter.format(tp.getLowerBound());
	}

	private List<Expression> buildTemporalExpression(String temporalProperties) {
		List<Expression> temporalExpressions = new ArrayList<Expression>();
		String[] temporalFeatures = temporalProperties.split("-");
		int length = (temporalFeatures.length <= 4) ? temporalFeatures.length : 4;
		for (int i = 0; i < length; i++) {
			if (temporalFeatures[i].charAt(0) != 'x') {
				String temporalFeature = temporalFeatures[i];
				if (temporalFeature.charAt(0) == '0')
					temporalFeature = temporalFeature.substring(1);
				Feature feature = null;
				switch (i) {
				case 0:
					feature = new Feature(TEMPORAL_YEAR_FEATURE, temporalFeature);
					break;
				case 1:
					feature = new Feature(TEMPORAL_MONTH_FEATURE, temporalFeature);
					break;
				case 2:
					feature = new Feature(TEMPORAL_DAY_FEATURE, temporalFeature);
					break;
				case 3:
					feature = new Feature(TEMPORAL_HOUR_FEATURE, temporalFeature);
					break;
				}
				temporalExpressions.add(new Expression(Operator.EQUAL, feature));
			}
		}
		return temporalExpressions;
	}

	private String getGroupKey(Path<Feature, String> path, String space) {
		if (null != path && path.hasPayload()) {
			List<Feature> labels = path.getLabels();
			String year = "xxxx", month = "xx", day = "xx", hour = "xx";
			int allset = (space == null) ? 0 : 1;
			for (Feature label : labels) {
				switch (label.getName().toLowerCase()) {
				case TEMPORAL_YEAR_FEATURE:
					year = label.getString();
					allset++;
					break;
				case TEMPORAL_MONTH_FEATURE:
					month = label.getString();
					allset++;
					break;
				case TEMPORAL_DAY_FEATURE:
					day = label.getString();
					allset++;
					break;
				case TEMPORAL_HOUR_FEATURE:
					hour = label.getString();
					allset++;
					break;
				case SPATIAL_FEATURE:
					if (space == null) {
						space = label.getString();
						allset++;
					}
					break;
				}
				if (allset == 5)
					break;
			}
			return String.format("%s-%s-%s-%s-%s", year, month, day, hour, space);
		}
		return String.format("%s-%s", getTemporalString(null), (space == null) ? getSpatialString(null) : space);
	}

	private String getSpaceKey(Path<Feature, String> path) {
		if (null != path && path.hasPayload()) {
			List<Feature> labels = path.getLabels();
			for (Feature label : labels)
				if (label.getName().toLowerCase().equals(SPATIAL_FEATURE))
					return label.getString();
		}
		return getSpatialString(null);
	}

	private Query queryIntersection(Query q1, Query q2) {
		if (q1 != null && q2 != null) {
			Query query = new Query();
			for (Operation q1Op : q1.getOperations()) {
				for (Operation q2Op : q2.getOperations()) {
					Operation op = new Operation(q1Op.getExpressions());
					op.addExpressions(q2Op.getExpressions());
					query.addOperation(op);
				}
			}
			logger.info(query.toString());
			return query;
		} else if (q1 != null) {
			return q1;
		} else if (q2 != null) {
			return q2;
		} else {
			return null;
		}
	}

	private class ParallelQueryEvaluator implements Runnable {
		private List<Operation> operations;
		private List<Path<Feature, String>> resultPaths;

		public ParallelQueryEvaluator(List<Operation> operations) {
			this.operations = operations;
		}

		public List<Path<Feature, String>> getResults() {
			return this.resultPaths;
		}

		@Override
		public void run() {
			Query query = new Query();
			query.addAllOperations(operations);
			this.resultPaths = metadataGraph.evaluateQuery(query);
		}
	}

	private List<Path<Feature, String>> executeParallelQuery(Query finalQuery) throws InterruptedException {
		logger.info("Query: " + finalQuery.toString());
		List<Path<Feature, String>> paths = new ArrayList<>();
		List<Operation> operations = finalQuery.getOperations();
		if (operations.size() > 0) {
			if (operations.size() > numCores) {
				int subsetSize = operations.size() / numCores;
				for (int i = 0; i < numCores; i++) {
					operations.subList(i, (i + 1) * subsetSize);
				}
				int size = operations.size();
				ExecutorService executor = Executors.newFixedThreadPool(numCores);
				List<ParallelQueryEvaluator> queryEvaluators = new ArrayList<>();
				for (int i = 0; i < numCores; i++) {
					int from = i * subsetSize;
					int to = (i + 1 != numCores) ? (i + 1) * subsetSize : size;
					List<Operation> subset = new ArrayList<>(operations.subList(from, to));
					ParallelQueryEvaluator pqe = new ParallelQueryEvaluator(subset);
					queryEvaluators.add(pqe);
					executor.execute(pqe);
				}
				operations.clear();
				executor.shutdown();
				boolean termination = executor.awaitTermination(10, TimeUnit.MINUTES);
				if (!termination)
					logger.severe("Query failed to process in 10 minutes");
				paths = new ArrayList<>();
				for (ParallelQueryEvaluator pqe : queryEvaluators)
					if (pqe.getResults() != null)
						paths.addAll(pqe.getResults());
			} else {
				paths = metadataGraph.evaluateQuery(finalQuery);
			}
		}
		return paths;
	}

	public Map<String, List<String>> listBlocks(String temporalProperties, List<Coordinates> spatialProperties,
			Query metaQuery, boolean group) throws InterruptedException {
		Map<String, List<String>> blockMap = new HashMap<String, List<String>>();
		String space = null;
		List<Path<Feature, String>> paths = null;
		List<String> blocks = new ArrayList<String>();
		if (temporalProperties != null && spatialProperties != null) {
			SpatialProperties sp = new SpatialProperties(new SpatialRange(spatialProperties));
			List<Coordinates> geometry = sp.getSpatialRange().hasPolygon() ? sp.getSpatialRange().getPolygon()
					: sp.getSpatialRange().getBounds();
			space = getSpatialString(sp);
			List<String> hashLocations = new ArrayList<>(Arrays.asList(GeoHash.getIntersectingGeohashes(geometry)));
			hashLocations.retainAll(this.geohashIndex);
			logger.info("baseLocations: " + hashLocations);
			Query query = new Query();
			List<Expression> temporalExpressions = buildTemporalExpression(temporalProperties);
			Polygon polygon = GeoHash.buildAwtPolygon(geometry);
			for (String geohash : hashLocations) {
				Set<GeoHash> intersections = new HashSet<>();
				String pattern = "%" + (geohash.length() * GeoHash.BITS_PER_CHAR) + "s";
				String binaryHash = String.format(pattern, Long.toBinaryString(GeoHash.hashToLong(geohash)));
				GeoHash.getGeohashPrefixes(polygon, new GeoHash(binaryHash.replace(" ", "0")),
						this.geohashPrecision * GeoHash.BITS_PER_CHAR, intersections);
				logger.info("baseHash: " + geohash + ", intersections: " + intersections.size());
				for (GeoHash gh : intersections) {
					String[] hashRange = gh.getValues(this.geohashPrecision);
					if (hashRange != null) {
						Operation op = new Operation(temporalExpressions);
						if (hashRange.length == 1)
							op.addExpressions(
									new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
						else {
							op.addExpressions(
									new Expression(Operator.GREATEREQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
							op.addExpressions(
									new Expression(Operator.LESSEQUAL, new Feature(SPATIAL_FEATURE, hashRange[1])));
						}
						query.addOperation(op);
					}
				}
			}
			paths = executeParallelQuery(queryIntersection(query, metaQuery));
		} else if (temporalProperties != null) {
			List<Expression> temporalExpressions = buildTemporalExpression(temporalProperties);
			Query query = new Query(
					new Operation(temporalExpressions.toArray(new Expression[temporalExpressions.size()])));
			paths = metadataGraph.evaluateQuery(queryIntersection(query, metaQuery));
		} else if (spatialProperties != null) {
			SpatialProperties sp = new SpatialProperties(new SpatialRange(spatialProperties));
			List<Coordinates> geometry = sp.getSpatialRange().hasPolygon() ? sp.getSpatialRange().getPolygon()
					: sp.getSpatialRange().getBounds();
			space = getSpatialString(sp);
			List<String> hashLocations = new ArrayList<>(Arrays.asList(GeoHash.getIntersectingGeohashes(geometry)));
			hashLocations.retainAll(this.geohashIndex);
			logger.info("baseLocations: " + hashLocations);
			Query query = new Query();
			Polygon polygon = GeoHash.buildAwtPolygon(geometry);
			for (String geohash : hashLocations) {
				Set<GeoHash> intersections = new HashSet<>();
				String pattern = "%" + (geohash.length() * GeoHash.BITS_PER_CHAR) + "s";
				String binaryHash = String.format(pattern, Long.toBinaryString(GeoHash.hashToLong(geohash)));
				GeoHash.getGeohashPrefixes(polygon, new GeoHash(binaryHash.replace(" ", "0")),
						this.geohashPrecision * GeoHash.BITS_PER_CHAR, intersections);
				logger.info("baseHash: " + geohash + ", intersections: " + intersections.size());
				for (GeoHash gh : intersections) {
					String[] hashRange = gh.getValues(this.geohashPrecision);
					if (hashRange != null) {
						Operation op = new Operation();
						if (hashRange.length == 1)
							op.addExpressions(
									new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
						else {
							op.addExpressions(
									new Expression(Operator.GREATEREQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
							op.addExpressions(
									new Expression(Operator.LESSEQUAL, new Feature(SPATIAL_FEATURE, hashRange[1])));
						}
						query.addOperation(op);
					}
				}
			}
			paths = executeParallelQuery(queryIntersection(query, metaQuery));
		} else {
			// non-chronal non-spatial
			paths = (metaQuery == null) ? metadataGraph.getAllPaths() : executeParallelQuery(metaQuery);
		}
		for (Path<Feature, String> path : paths) {
			String groupKey = group ? getGroupKey(path, space) : getSpaceKey(path);
			blocks = blockMap.get(groupKey);
			if (blocks == null) {
				blocks = new ArrayList<String>();
				blockMap.put(groupKey, blocks);
			}
			blocks.addAll(path.getPayload());
		}
		return blockMap;
	}

	private class Tracker {
		private int occurrence;
		private long fileSize;
		private long timestamp;

		public Tracker(long filesize, long millis) {
			this.occurrence = 1;
			this.fileSize = filesize;
			this.timestamp = millis;
		}

		public void incrementOccurrence() {
			this.occurrence++;
		}

		public int getOccurrence() {
			return this.occurrence;
		}

		public void incrementFilesize(long value) {
			this.fileSize += value;
		}

		public long getFilesize() {
			return this.fileSize;
		}

		public void updateTimestamp(long millis) {
			if (this.timestamp < millis)
				this.timestamp = millis;
		}

		public long getTimestamp() {
			return this.timestamp;
		}
	}

	public JSONArray getOverview() {
		JSONArray overviewJSON = new JSONArray();
		Map<String, Tracker> geohashMap = new HashMap<String, Tracker>();
		Calendar timestamp = Calendar.getInstance();
		timestamp.setTimeZone(TemporalHash.TIMEZONE);
		List<Path<Feature, String>> allPaths = metadataGraph.getAllPaths();
		logger.info("all paths size: " + allPaths.size());
		try {
			for (Path<Feature, String> path : allPaths) {
				long payloadSize = 0;
				if (path.hasPayload()) {
					for (String payload : path.getPayload()) {
						try {
							payloadSize += Files.size(java.nio.file.Paths.get(payload));
						} catch (IOException e) { /* e.printStackTrace(); */
							System.err.println("Exception occurred reading the block size. " + e.getMessage());
						}
					}
				}
				String geohash = path.get(4).getLabel().getString();
				String yearFeature = path.get(0).getLabel().getString();
				String monthFeature = path.get(1).getLabel().getString();
				String dayFeature = path.get(2).getLabel().getString();
				String hourFeature = path.get(3).getLabel().getString();
				if (yearFeature.charAt(0) == 'x') {
					System.err.println("Cannot build timestamp without year. Ignoring path");
					continue;
				}
				if (monthFeature.charAt(0) == 'x')
					monthFeature = "12";
				if (hourFeature.charAt(0) == 'x')
					hourFeature = "23";
				int year = Integer.parseInt(yearFeature);
				int month = Integer.parseInt(monthFeature) - 1;
				if (dayFeature.charAt(0) == 'x') {
					Calendar cal = Calendar.getInstance();
					cal.setTimeZone(TemporalHash.TIMEZONE);
					cal.set(Calendar.YEAR, year);
					cal.set(Calendar.MONTH, month);
					dayFeature = String.valueOf(cal.getActualMaximum(Calendar.DAY_OF_MONTH));
				}
				int day = Integer.parseInt(dayFeature);
				int hour = Integer.parseInt(hourFeature);
				timestamp.set(year, month, day, hour, 59, 59);

				Tracker geohashTracker = geohashMap.get(geohash);
				if (geohashTracker == null) {
					geohashMap.put(geohash, new Tracker(payloadSize, timestamp.getTimeInMillis()));
				} else {
					geohashTracker.incrementOccurrence();
					geohashTracker.incrementFilesize(payloadSize);
					geohashTracker.updateTimestamp(timestamp.getTimeInMillis());
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "failed to process a path", e);
		}

		logger.info("geohash map size: " + geohashMap.size());
		for (String geohash : geohashMap.keySet()) {
			Tracker geohashTracker = geohashMap.get(geohash);
			JSONObject geohashJSON = new JSONObject();
			geohashJSON.put("region", geohash);
			List<Coordinates> boundingBox = GeoHash.decodeHash(geohash).getBounds();
			JSONArray bbJSON = new JSONArray();
			for (Coordinates coordinates : boundingBox) {
				JSONObject vertex = new JSONObject();
				vertex.put("lat", coordinates.getLatitude());
				vertex.put("lng", coordinates.getLongitude());
				bbJSON.put(vertex);
			}
			geohashJSON.put("spatialCoordinates", bbJSON);
			geohashJSON.put("blockCount", geohashTracker.getOccurrence());
			geohashJSON.put("fileSize", geohashTracker.getFilesize());
			geohashJSON.put("latestTimestamp", geohashTracker.getTimestamp());
			overviewJSON.put(geohashJSON);
		}
		return overviewJSON;
	}

	/**
	 * Using the Feature attributes found in the provided Metadata, a path is
	 * created for insertion into the Metadata Graph.
	 */
	protected FeaturePath<String> createPath(String physicalPath, Metadata meta) {
		FeaturePath<String> path = new FeaturePath<String>(physicalPath, meta.getAttributes().toArray());
		return path;
	}

	@Override
	public void storeMetadata(Metadata metadata, String blockPath) throws FileSystemException, IOException {
		FeaturePath<String> path = createPath(blockPath, metadata);
		pathJournal.persistPath(path);
		storePath(path);
	}

	private void storePath(FeaturePath<String> path) throws FileSystemException {
		try {
			metadataGraph.addPath(path);
		} catch (Exception e) {
			throw new FileSystemException("Error storing metadata: " + e.getClass().getCanonicalName(), e);
		}
	}

	public MetadataGraph getMetadataGraph() {
		return metadataGraph;
	}

	public List<Path<Feature, String>> query(Query query) {
		return metadataGraph.evaluateQuery(query);
	}

	private List<String[]> getFeaturePaths(String blockPath) throws IOException {
		byte[] blockBytes = Files.readAllBytes(Paths.get(blockPath));
		String blockData = new String(blockBytes, "UTF-8");
		List<String[]> paths = new ArrayList<String[]>();
		String[] lines = blockData.split("\\r?\\n");
		int splitLimit = this.featureList.size();
		for (String line : lines)
			paths.add(line.split(",", splitLimit));
		return paths;
	}

	private boolean isGridInsidePolygon(GeoavailabilityGrid grid, GeoavailabilityQuery geoQuery) {
		Polygon polygon = new Polygon();
		for (Coordinates coords : geoQuery.getPolygon()) {
			Point<Integer> point = GeoHash.coordinatesToXY(coords);
			polygon.addPoint(point.X(), point.Y());
		}
		logger.info("checking geohash " + grid.getBaseHash() + " intersection with the polygon");
		SpatialRange hashRange = grid.getBaseRange();
		Pair<Coordinates, Coordinates> pair = hashRange.get2DCoordinates();
		Point<Integer> upperLeft = GeoHash.coordinatesToXY(pair.a);
		Point<Integer> lowerRight = GeoHash.coordinatesToXY(pair.b);
		if (polygon.contains(new Rectangle(upperLeft.X(), upperLeft.Y(), lowerRight.X() - upperLeft.X(),
				lowerRight.Y() - upperLeft.Y())))
			return true;
		return false;
	}

	private class ParallelQueryProcessor implements Runnable {
		private List<String[]> featurePaths;
		private Query query;
		private GeoavailabilityGrid grid;
		private Bitmap queryBitmap;
		private String storagePath;

		public ParallelQueryProcessor(List<String[]> featurePaths, Query query, GeoavailabilityGrid grid,
				Bitmap queryBitmap, String storagePath) {
			this.featurePaths = featurePaths;
			this.query = query;
			this.grid = grid;
			this.queryBitmap = queryBitmap;
			this.storagePath = storagePath + BLOCK_EXTENSION;
		}

		@Override
		public void run() {
			try {
				if (queryBitmap != null) {
					int latOrder = -1, lngOrder = -1, index = 0;
					for (Pair<String, FeatureType> columnPair : GeospatialFileSystem.this.featureList) {
						if (columnPair.a.equalsIgnoreCase(GeospatialFileSystem.this.spatialHint.getLatitudeHint()))
							latOrder = index++;
						else if (columnPair.a
								.equalsIgnoreCase(GeospatialFileSystem.this.spatialHint.getLongitudeHint()))
							lngOrder = index++;
						else
							index++;
					}

					GeoavailabilityMap<String[]> geoMap = new GeoavailabilityMap<String[]>(grid);
					Iterator<String[]> pathIterator = this.featurePaths.iterator();
					while (pathIterator.hasNext()) {
						String[] features = pathIterator.next();
						float lat = Math.getFloat(features[latOrder]);
						float lon = Math.getFloat(features[lngOrder]);
						if (!Float.isNaN(lat) && !Float.isNaN(lon))
							geoMap.addPoint(new Coordinates(lat, lon), features);
						pathIterator.remove();
					}
					for (List<String[]> paths : geoMap.query(queryBitmap).values())
						this.featurePaths.addAll(paths);
				}
				if (query != null && this.featurePaths.size() > 0) {
					MetadataGraph temporaryGraph = new MetadataGraph();
					Iterator<String[]> pathIterator = this.featurePaths.iterator();
					while (pathIterator.hasNext()) {
						String[] features = pathIterator.next();
						try {
							Metadata metadata = new Metadata();
							FeatureSet featureset = new FeatureSet();
							for (int i = 0; i < features.length; i++) {
								Pair<String, FeatureType> pair = GeospatialFileSystem.this.featureList.get(i);
								if (pair.b == FeatureType.FLOAT)
									featureset.put(new Feature(pair.a, Math.getFloat(features[i])));
								if (pair.b == FeatureType.INT)
									featureset.put(new Feature(pair.a, Math.getInteger(features[i])));
								if (pair.b == FeatureType.LONG)
									featureset.put(new Feature(pair.a, Math.getLong(features[i])));
								if (pair.b == FeatureType.DOUBLE)
									featureset.put(new Feature(pair.a, Math.getDouble(features[i])));
								if (pair.b == FeatureType.STRING)
									featureset.put(new Feature(pair.a, features[i]));
							}
							metadata.setAttributes(featureset);
							Path<Feature, String> featurePath = createPath("/nopath", metadata);
							temporaryGraph.addPath(featurePath);
						} catch (Exception e) {
							logger.warning(e.getMessage());
						}
						pathIterator.remove();
					}
					List<Path<Feature, String>> evaluatedPaths = temporaryGraph.evaluateQuery(query);
					for (Path<Feature, String> path : evaluatedPaths) {
						String[] featureValues = new String[path.size()];
						int index = 0;
						for (Feature feature : path.getLabels())
							featureValues[index++] = feature.getString();
						this.featurePaths.add(featureValues);
					}
				}

				if (featurePaths.size() > 0) {
					try (FileOutputStream fos = new FileOutputStream(this.storagePath)) {
						Iterator<String[]> pathIterator = featurePaths.iterator();
						while (pathIterator.hasNext()) {
							String[] path = pathIterator.next();
							StringBuffer pathSB = new StringBuffer();
							for (int j = 0; j < path.length; j++) {
								pathSB.append(path[j]);
								if (j + 1 != path.length)
									pathSB.append(",");
							}
							fos.write(pathSB.toString().getBytes("UTF-8"));
							pathIterator.remove();
							if (pathIterator.hasNext())
								fos.write("\n".getBytes("UTF-8"));
						}
					}
				} else {
					this.storagePath = null;
				}
			} catch (IOException | BitmapException e) {
				logger.log(Level.SEVERE, "Something went wrong while querying the filesystem.", e);
				this.storagePath = null;
			}
		}

		public String getStoragePath() {
			return this.storagePath;
		}

	}

	public List<String> query(String blockPath, GeoavailabilityQuery geoQuery, GeoavailabilityGrid grid,
			Bitmap queryBitmap, String pathPrefix) throws IOException, InterruptedException {
		List<String> resultFiles = new ArrayList<>();
		List<String[]> featurePaths = null;
		boolean skipGridProcessing = false;
		if (geoQuery.getPolygon() != null && geoQuery.getQuery() != null) {
			skipGridProcessing = isGridInsidePolygon(grid, geoQuery);
			featurePaths = getFeaturePaths(blockPath);
		} else if (geoQuery.getPolygon() != null) {
			skipGridProcessing = isGridInsidePolygon(grid, geoQuery);
			if (!skipGridProcessing)
				featurePaths = getFeaturePaths(blockPath);
		} else if (geoQuery.getQuery() != null) {
			featurePaths = getFeaturePaths(blockPath);
		} else {
			resultFiles.add(blockPath);
			return resultFiles;
		}

		if (featurePaths == null) {
			resultFiles.add(blockPath);
			return resultFiles;
		}

		queryBitmap = skipGridProcessing ? null : queryBitmap;
		int size = featurePaths.size();
		int partition = java.lang.Math.max(size / numCores, MIN_GRID_POINTS);
		int parallelism = java.lang.Math.min(size / partition, numCores);
		if (parallelism > 1) {
			ExecutorService executor = Executors.newFixedThreadPool(parallelism);
			List<ParallelQueryProcessor> queryProcessors = new ArrayList<>();
			for (int i = 0; i < parallelism; i++) {
				int from = i * partition;
				int to = (i + 1 != parallelism) ? (i + 1) * partition : size;
				List<String[]> subset = new ArrayList<>(featurePaths.subList(from, to));
				ParallelQueryProcessor pqp = new ParallelQueryProcessor(subset, geoQuery.getQuery(), grid, queryBitmap,
						pathPrefix + "-" + i);
				queryProcessors.add(pqp);
				executor.execute(pqp);
			}
			featurePaths.clear();
			executor.shutdown();
			executor.awaitTermination(10, TimeUnit.MINUTES);
			for (ParallelQueryProcessor pqp : queryProcessors)
				if (pqp.getStoragePath() != null)
					resultFiles.add(pqp.getStoragePath());
		} else {
			ParallelQueryProcessor pqp = new ParallelQueryProcessor(featurePaths, geoQuery.getQuery(), grid,
					queryBitmap, pathPrefix);
			pqp.run(); // to avoid another thread creation
			if (pqp.getStoragePath() != null)
				resultFiles.add(pqp.getStoragePath());
		}

		return resultFiles;
	}

	public JSONArray getFeaturesJSON() {
		return metadataGraph.getFeaturesJSON();
	}

	@Override
	public void shutdown() {
		logger.info("FileSystem shutting down");
		try {
			pathJournal.shutdown();
		} catch (Exception e) {
			/* Everything is going down here, just print out the error */
			e.printStackTrace();
		}
	}
}
