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
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

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
	public static final int MAX_GRID_POINTS = 100000;

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
		if (featureList != null) {
			this.featureList = new ArrayList<>();
			for (String nameType : featureList.split(",")) {
				String[] pair = nameType.split(":");
				this.featureList
						.add(new Pair<String, FeatureType>(pair[0], FeatureType.fromInt(Integer.parseInt(pair[1]))));
			}
		}
		this.spatialHint = sHint;
		if (this.featureList != null && this.spatialHint == null)
			throw new IllegalArgumentException("Spatial hint is needed when feature list is provided");
		this.storageRoot = storageDirectory;
		this.temporalType = TemporalType.fromType(temporalType);

		if (nodesPerGroup <= 0)
			this.network = networkInfo;
		else {
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

		/* Ensure the storage directory is there. */
		File blockDirectory = new File(blockDirPath);
		if (!blockDirectory.exists()) {
			if (!blockDirectory.mkdirs()) {
				throw new IOException("Failed to create directory (" + blockDirPath + ") for block.");
			}
		}

		String blockString = new String(block.getData(), "UTF-8");

		// Adding temporal and spatial features at the top to the existing
		// attributes
		FeatureSet newfs = new FeatureSet();
		String[] temporalFeature = time.split("-");
		newfs.put(new Feature(TEMPORAL_YEAR_FEATURE, temporalFeature[0]));
		newfs.put(new Feature(TEMPORAL_MONTH_FEATURE, temporalFeature[1]));
		newfs.put(new Feature(TEMPORAL_DAY_FEATURE, temporalFeature[2]));
		newfs.put(new Feature(TEMPORAL_HOUR_FEATURE, temporalFeature[3]));
		newfs.put(new Feature(SPATIAL_FEATURE, getSpatialString(meta.getSpatialProperties())));
		for (Feature feature : meta.getAttributes())
			newfs.put(feature);
		meta.setAttributes(newfs);

		File gblock = new File(blockPath);
		if (gblock.exists()) {
			java.nio.file.Path gblockPath = Paths.get(blockPath);
			byte[] blockData = Files.readAllBytes(gblockPath);
			try {
				Block existingBlock = Serializer.deserialize(Block.class, blockData);
				StringBuffer dataBuffer = new StringBuffer();
				dataBuffer.append(new String(existingBlock.getData(), "UTF-8"));
				dataBuffer.append('\n');
				dataBuffer.append(blockString);
				// over-write the existing block with new blocks data. metadata
				// is not changed.
				block = new Block(existingBlock.getFilesystem(), existingBlock.getMetadata(),
						dataBuffer.toString().getBytes("UTF-8"));
			} catch (SerializationException e) {
				throw new IOException("Failed to deserialize the existing block - " + e.getMessage(), e.getCause());
			}
		} else {
			try {
				storeMetadata(meta, blockPath);
			} catch (Exception e) {
				throw new FileSystemException("Error storing block: " + e.getClass().getCanonicalName(), e);
			}
		}
		FileOutputStream blockOutStream = new FileOutputStream(blockPath);
		byte[] blockData = Serializer.serialize(block);
		blockOutStream.write(blockData);
		blockOutStream.close();

		if (latestTime == null || latestTime.getEnd() < meta.getTemporalProperties().getEnd()) {
			this.latestTime = meta.getTemporalProperties();
			this.latestSpace = geohash;
		}

		if (earliestTime == null || earliestTime.getStart() > meta.getTemporalProperties().getStart()) {
			this.earliestTime = meta.getTemporalProperties();
			this.earliestSpace = geohash;
		}

		return blockPath;
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
		if (q1 == null && q2 == null)
			return null;
		else if (q1 != null && q2 == null)
			return q1;
		else if (q1 == null && q2 != null)
			return q2;
		else {
			Query query = new Query();
			for (Operation q1Op : q1.getOperations()) {
				for (Operation q2Op : q2.getOperations()) {
					Operation op = new Operation(q1Op.getExpressions());
					op.addExpressions(q2Op.getExpressions());
					query.addOperation(op);
				}
			}
			return query;
		}
	}

	public Map<String, List<String>> listBlocks(String temporalProperties, List<Coordinates> spatialProperties,
			Query metaQuery, boolean group) {
		Map<String, List<String>> blockMap = new HashMap<String, List<String>>();
		String space = null;
		List<Path<Feature, String>> paths = null;
		List<String> blocks = new ArrayList<String>();
		if (temporalProperties != null && spatialProperties != null) {
			SpatialProperties sp = new SpatialProperties(new SpatialRange(spatialProperties));
			space = getSpatialString(sp);
			String[] geohashes = new String[] {};
			if (sp.hasRange()) {
				geohashes = sp.getSpatialRange().hasPolygon()
						? GeoHash.getIntersectingGeohashes(sp.getSpatialRange().getPolygon(), this.geohashPrecision)
						: GeoHash.getIntersectingGeohashes(sp.getSpatialRange().getBounds(), this.geohashPrecision);
			}

			Query query = new Query();
			List<Expression> temporalExpressions = buildTemporalExpression(temporalProperties);
			for (String geohash : geohashes) {
				Operation op = new Operation(temporalExpressions);
				op.addExpressions(new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, geohash)));
				query.addOperation(op);
			}
			paths = metadataGraph.evaluateQuery(queryIntersection(query, metaQuery));
		} else if (temporalProperties != null) {
			List<Expression> temporalExpressions = buildTemporalExpression(temporalProperties);
			Query query = new Query(
					new Operation(temporalExpressions.toArray(new Expression[temporalExpressions.size()])));
			paths = metadataGraph.evaluateQuery(queryIntersection(query, metaQuery));
		} else if (spatialProperties != null) {
			SpatialProperties sp = new SpatialProperties(new SpatialRange(spatialProperties));
			String[] geohashes = new String[] {};
			if (sp.hasRange()) {
				geohashes = sp.getSpatialRange().hasPolygon()
						? GeoHash.getIntersectingGeohashes(sp.getSpatialRange().getPolygon(), this.geohashPrecision)
						: GeoHash.getIntersectingGeohashes(sp.getSpatialRange().getBounds(), this.geohashPrecision);
			}
			space = getSpatialString(sp);
			Query query = new Query();
			for (String geohash : geohashes)
				query.addOperation(
						new Operation(new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, geohash))));
			paths = metadataGraph.evaluateQuery(queryIntersection(query, metaQuery));
		} else {
			// non-chronal non-spatial
			paths = (metaQuery == null) ? metadataGraph.getAllPaths() : metadataGraph.evaluateQuery(metaQuery);
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

	public List<Path<Feature, String>> query(String blockPath, Query query) throws IOException {
		List<Path<Feature, String>> featurePaths = new ArrayList<Path<Feature, String>>();
		try {
			logger.info("querying filesystem " + this.name + " for block path - " + blockPath);
			if (this.featureList == null) {
				logger.log(Level.SEVERE,
						"This method should not be called for blocks not having point feature data stored");
				return featurePaths;
			}
			if (this.spatialHint == null) {
				logger.log(Level.SEVERE, "No spatial hint present for the filesystem - " + this.name);
				return featurePaths;
			}
			byte[] blockBytes = Files.readAllBytes(Paths.get(blockPath));
			Block block = Serializer.deserialize(Block.class, blockBytes);
			String blockData = new String(block.getData(), "UTF-8");

			MetadataGraph temporaryGraph = new MetadataGraph();
			int splitLimit = this.featureList.size();
			for (String line : blockData.split("\\r?\\n")) {
				try {
					String[] features = line.split(",", splitLimit);
					Metadata metadata = new Metadata();
					FeatureSet featureset = new FeatureSet();
					for (int i = 0; i < features.length; i++) {
						// first two features are special reserved
						// attributes - hence i+2
						Pair<String, FeatureType> pair = this.featureList.get(i);
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
					Path<Feature, String> featurePath = createPath(blockPath, metadata);
					temporaryGraph.addPath(featurePath);
				} catch (Exception e) {
					logger.warning(e.getMessage());
				}
			}
			logger.info("Built temporary metadata graph");
			featurePaths = query != null ? temporaryGraph.evaluateQuery(query) : temporaryGraph.getAllPaths();
			logger.info("Number of paths in the considered block - " + featurePaths.size());
		} catch (SerializationException | IOException e) {
			logger.log(Level.SEVERE, "Failed to query for the given block(" + blockPath + ") - " + e.getMessage(), e);
		}
		return featurePaths;
	}

	public List<Path<Feature, String>> query(String geohash, List<Path<Feature, String>> featurePaths,
			List<Coordinates> queryPolygon) throws IOException {
		if (featurePaths != null && featurePaths.size() > 0 && queryPolygon != null) {
			List<Path<Feature, String>> resultPaths = new ArrayList<Path<Feature, String>>();
			Polygon polygon = new Polygon();
			for (Coordinates coords : queryPolygon) {
				Point<Integer> point = GeoHash.coordinatesToXY(coords);
				polygon.addPoint(point.X(), point.Y());
			}
			logger.info("checking geohash " + geohash + " intersection with the polygon");
			SpatialRange hashRange = GeoHash.decodeHash(geohash);
			Pair<Coordinates, Coordinates> pair = hashRange.get2DCoordinates();
			Point<Integer> upperLeft = GeoHash.coordinatesToXY(pair.a);
			Point<Integer> lowerRight = GeoHash.coordinatesToXY(pair.b);
			if (polygon.contains(new Rectangle(upperLeft.X(), upperLeft.Y(), lowerRight.X() - upperLeft.X(),
					lowerRight.Y() - upperLeft.Y())))
				return featurePaths;
			else {
				int latOrder = -1, lngOrder = -1, index = 0;
				for (Pair<String, FeatureType> columnPair : this.featureList) {
					if (columnPair.a.equalsIgnoreCase(this.spatialHint.getLatitudeHint()))
						latOrder = index++;
					else if (columnPair.a.equalsIgnoreCase(this.spatialHint.getLongitudeHint()))
						lngOrder = index++;
					else
						index++;
				}

				if (latOrder != -1 && lngOrder != -1) {
					GeoavailabilityMap<Path<Feature, String>> geoMap = new GeoavailabilityMap<Path<Feature, String>>(
							geohash, GeoHash.MAX_PRECISION * 2 / 3);
					for (Path<Feature, String> fpath : featurePaths) {
						float lat = fpath.get(latOrder).getLabel().getFloat();
						float lon = fpath.get(lngOrder).getLabel().getFloat();
						if (!Float.isNaN(lat) && !Float.isNaN(lon))
							geoMap.addPoint(new Coordinates(lat, lon), fpath);
					}
					try {
						GeoavailabilityQuery geoQuery = new GeoavailabilityQuery(queryPolygon);
						for (List<Path<Feature, String>> paths : geoMap.query(geoQuery).values())
							resultPaths.addAll(paths);
						logger.info(
								"Number of paths in the geoavailability grid(" + geohash + ") - " + resultPaths.size());
					} catch (Exception e) {
						logger.log(Level.SEVERE, "Something went wrong while querying on the grid", e);
					}
				} else {
					logger.log(Level.SEVERE, "Failed to identify the positions of spatial hint. No paths are returned");
				}
				return resultPaths;
			}
		}
		return featurePaths;
	}

	/*
	 * public List<Path<Feature, String>> query(String blockPath,
	 * GeoavailabilityQuery geoQuery) throws IOException { try { logger.info(
	 * "querying filesystem " + this.name + " for block path - " + blockPath);
	 * List<Path<Feature, String>> featurePaths = new ArrayList<Path<Feature,
	 * String>>(); if (this.featureList == null) { logger.log(Level.SEVERE,
	 * "This method should not be called for blocks not having point feature data stored"
	 * ); return featurePaths; } if (this.spatialHint == null) {
	 * logger.log(Level.SEVERE, "No spatial hint present for the filesystem - "
	 * + this.name); return featurePaths; } byte[] blockBytes =
	 * Files.readAllBytes(Paths.get(blockPath)); Block block =
	 * Serializer.deserialize(Block.class, blockBytes); String blockData = new
	 * String(block.getData(), "UTF-8");
	 * 
	 * MetadataGraph temporaryGraph = new MetadataGraph();
	 * 
	 * int latOrder = -1; int longOrder = -1; for (String line :
	 * blockData.split("\\r?\\n")) { try { String[] features = line.split(",");
	 * Metadata metadata = new Metadata(); FeatureSet featureset = new
	 * FeatureSet(); for (int i = 0; i < features.length; i++) { // first two
	 * features are special reserved // attributes - hence i+2 Pair<String,
	 * FeatureType> pair = this.featureList.get(i); if (latOrder == -1 &&
	 * pair.a.equalsIgnoreCase(this.spatialHint.getLatitudeHint())) latOrder =
	 * i; if (longOrder == -1 &&
	 * pair.a.equalsIgnoreCase(this.spatialHint.getLongitudeHint())) longOrder =
	 * i; if (pair.b == FeatureType.FLOAT) featureset.put(new Feature(pair.a,
	 * Math.getFloat(features[i]))); if (pair.b == FeatureType.INT)
	 * featureset.put(new Feature(pair.a, Math.getInteger(features[i]))); if
	 * (pair.b == FeatureType.LONG) featureset.put(new Feature(pair.a,
	 * Math.getLong(features[i]))); if (pair.b == FeatureType.DOUBLE)
	 * featureset.put(new Feature(pair.a, Math.getDouble(features[i]))); if
	 * (pair.b == FeatureType.STRING) featureset.put(new Feature(pair.a,
	 * features[i])); } metadata.setAttributes(featureset); Path<Feature,
	 * String> featurePath = createPath(blockPath, metadata);
	 * temporaryGraph.addPath(featurePath); } catch (Exception e) {
	 * logger.warning(e.getMessage()); } } logger.info(
	 * "Built temporary metadata graph"); featurePaths = geoQuery.getQuery() !=
	 * null ? temporaryGraph.evaluateQuery(geoQuery.getQuery()) :
	 * temporaryGraph.getAllPaths(); if (geoQuery.getPolygon() != null) {
	 * Polygon polygon = new Polygon(); for (Coordinates coords :
	 * geoQuery.getPolygon()) { Point<Integer> point =
	 * GeoHash.coordinatesToXY(coords); polygon.addPoint(point.X(), point.Y());
	 * } int hashendIndex = blockPath.lastIndexOf(File.separator); String
	 * blockHash = blockPath.substring(hashendIndex - this.geohashPrecision,
	 * hashendIndex); logger.info("checking geohash " + blockHash +
	 * " intersection with the polygon"); SpatialRange hashRange =
	 * GeoHash.decodeHash(blockHash); Pair<Coordinates, Coordinates> pair =
	 * hashRange.get2DCoordinates(); Point<Integer> upperLeft =
	 * GeoHash.coordinatesToXY(pair.a); Point<Integer> lowerRight =
	 * GeoHash.coordinatesToXY(pair.b); if (polygon.contains(new
	 * Rectangle(upperLeft.X(), upperLeft.Y(), lowerRight.X() - upperLeft.X(),
	 * lowerRight.Y() - upperLeft.Y()))) return featurePaths; else { if
	 * (latOrder != -1 && longOrder != -1) { GeoavailabilityMap<Path<Feature,
	 * String>> geoMap = new GeoavailabilityMap<Path<Feature, String>>(
	 * blockHash, 18); for (Path<Feature, String> fpath : featurePaths) { float
	 * lat = fpath.get(latOrder).getLabel().getFloat(); float lon =
	 * fpath.get(longOrder).getLabel().getFloat(); if (!Float.isNaN(lat) &&
	 * !Float.isNaN(lon)) geoMap.addPoint(new Coordinates(lat, lon), fpath); }
	 * List<Path<Feature, String>> results = new ArrayList<Path<Feature,
	 * String>>(); for (List<Path<Feature, String>> paths :
	 * geoMap.query(geoQuery).values()) results.addAll(paths); logger.info(
	 * "Number of paths in the considered block - " + results.size()); return
	 * results; } } } logger.info("Number of paths in the considered block - " +
	 * featurePaths.size()); return featurePaths; } catch
	 * (SerializationException | IOException | BitmapException e) { throw new
	 * IOException("Failed to query for the given block(" + blockPath + ") - " +
	 * e.getMessage(), e.getCause()); } }
	 */

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
