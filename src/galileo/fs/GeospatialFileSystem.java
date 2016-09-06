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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;

import galileo.bmp.BitmapException;
import galileo.bmp.GeoavailabilityMap;
import galileo.bmp.GeoavailabilityQuery;
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
import galileo.graph.FeatureHierarchy;
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

	private static final String DEFAULT_TIME_FORMAT = "yyyy/M/d";
	private static final int DEFAULT_GEOHASH_PRECISION = 4;

	private static final String pathStore = "metadata.paths";

	private MetadataGraph metadataGraph;

	private PathJournal pathJournal;

	private SimpleDateFormat timeFormatter;
	private String timeFormat;
	private int geohashPrecision;
	private TemporalProperties lastModified;

	private static final String TEMPORAL_FEATURE = "x__temporal__x";
	private static final String SPATIAL_FEATURE = "x__spatial__x";

	public GeospatialFileSystem(String storageDirectory, String name)
			throws FileSystemException, IOException, SerializationException {
		super(storageDirectory, name);

		this.timeFormat = System.getProperty("galileo.fs.GeospatialFileSystem.timeFormat", DEFAULT_TIME_FORMAT);

		this.geohashPrecision = Integer.parseInt(System.getProperty("galileo.fs.GeospatialFileSystem.geohashPrecision",
				String.valueOf(DEFAULT_GEOHASH_PRECISION)));

		timeFormatter = new SimpleDateFormat();
		timeFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		timeFormatter.applyPattern(timeFormat);
		pathJournal = new PathJournal(this.storageDirectory + File.separator + pathStore);

		createMetadataGraph();
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

	public long getLastUpdated() {
		return this.lastModified.getStart();
	}

	private String getTemporalString(TemporalProperties tp) {
		if (tp == null)
			return "xxxx-xx-xx";
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis(tp.getStart());
		int day = c.get(Calendar.DAY_OF_MONTH);
		int month = c.get(Calendar.MONTH) + 1;
		int year = c.get(Calendar.YEAR);
		return String.format("%d-%d-%d", year, month, day);
	}

	private String getSpatialString(SpatialProperties sp) {
		char[] hash = new char[this.geohashPrecision];
		Arrays.fill(hash, 'x');
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
	 * Creates a new block if one does not exist based on the spatio-temporal
	 * properties of the metadata or appends the bytes to an existing block.
	 */
	@Override
	public String storeBlock(Block block) throws FileSystemException, IOException {
		if (lastModified == null || lastModified.getStart() < block.getMetadata().getTemporalProperties().getStart())
			lastModified = block.getMetadata().getTemporalProperties();
		String time = getTemporalString(block.getMetadata().getTemporalProperties());
		String geohash = getSpatialString(block.getMetadata().getSpatialProperties());
		String name = String.format("%s-%s", time, geohash);
		String blockDirPath = this.storageDirectory + File.separator + getStorageDirectory(block);
		String blockPath = blockDirPath + File.separator + name + FileSystem.BLOCK_EXTENSION;

		/* Ensure the storage directory is there. */
		File blockDirectory = new File(blockDirPath);
		if (!blockDirectory.exists()) {
			if (!blockDirectory.mkdirs()) {
				throw new IOException("Failed to create directory (" + blockDirPath + ") for block.");
			}
		}

		Metadata meta = block.getMetadata();
		String blockString = "";
		if (block.getData() == null) {
			FeatureSet featureSet = meta.getAttributes();
			for (Feature feature : featureSet)
				blockString += feature.dataToString() + ",";
			if (blockString.length() > 1)
				blockString = blockString.substring(0, blockString.length() - 1);
		} else {
			blockString = new String(block.getData(), "UTF-8");
		}

		// Adding temporal and spatial features at the top to the existing
		// attributes
		FeatureSet newfs = new FeatureSet();
		newfs.put(new Feature(TEMPORAL_FEATURE, getTemporalString(meta.getTemporalProperties())));
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
				block = new Block(existingBlock.getFileSystem(), existingBlock.getMetadata(),
						dataBuffer.toString().getBytes("UTF-8"));
			} catch (SerializationException e) {
				throw new IOException("Failed to deserialize the existing block - " + e.getMessage(), e.getCause());
			}
		} else {
			FeaturePath<String> path = createPath(blockPath, meta);
			try {
				metadataGraph.addPath(path);
			} catch (Exception e) {
				throw new FileSystemException("Error storing block: " + e.getClass().getCanonicalName(), e);
			}
		}
		FileOutputStream blockOutStream = new FileOutputStream(blockPath);
		byte[] blockData = Serializer.serialize(block);
		blockOutStream.write(blockData);
		blockOutStream.close();
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

	public Map<String, List<String>> listBlocks(Metadata metadata) {
			Map<String, List<String>> blockMap = new HashMap<String, List<String>>();
			List<String> blocks = new ArrayList<String>();
			if (metadata.hasTemporalProperties() && metadata.hasSpatialProperties()) {
				String time = getTemporalString(metadata.getTemporalProperties());
				String space = getSpatialString(metadata.getSpatialProperties());
				Query query = new Query(
						new Operation(new Expression(Operator.EQUAL, new Feature(TEMPORAL_FEATURE, time)),
								new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, space))));
				List<Path<Feature, String>> paths = metadataGraph.evaluateQuery(query);
				for (Path<Feature, String> path : paths)
					if (path.hasPayload())
						blocks.addAll(path.getPayload());
				blockMap.put(String.format("%s-%s", time, space), blocks);
				return blockMap;
			} else if (metadata.hasTemporalProperties()) {
				String time = getTemporalString(metadata.getTemporalProperties());
				Query query = new Query(
						new Operation(new Expression(Operator.EQUAL, new Feature(TEMPORAL_FEATURE, time))));
				List<Path<Feature, String>> paths = metadataGraph.evaluateQuery(query);
				for (Path<Feature, String> path : paths) {
					if (path.hasPayload()) {
						List<Feature> labels = path.getLabels();
						for (Feature label : labels) {
							if (SPATIAL_FEATURE.equalsIgnoreCase(label.getName())) {
								blocks = blockMap.get(String.format("%s-%s", time, label.getString()));
								if (blocks == null) {
									blocks = new ArrayList<String>();
									blockMap.put(String.format("%s-%s", time, label.getString()), blocks);
								}
								blocks.addAll(path.getPayload());
								break;
							}
						}
					}
				}
				return blockMap;
			} else if (metadata.hasSpatialProperties()) {
				SpatialProperties sp = metadata.getSpatialProperties();
				String[] geohashes = new String[] {};
				if (sp.hasRange()) {
					geohashes = sp.getSpatialRange().hasPolygon()
							? GeoHash.getIntersectingGeohashes(sp.getSpatialRange().getPolygon(), this.geohashPrecision)
							: GeoHash.getIntersectingGeohashes(sp.getSpatialRange().getBounds(), this.geohashPrecision);
				}
				String space = getSpatialString(sp);
				for (String geohash : geohashes) {
					Query query = new Query(
							new Operation(new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, geohash))));
					List<Path<Feature, String>> paths = metadataGraph.evaluateQuery(query);
					for (Path<Feature, String> path : paths) {
						if (path.hasPayload()) {
							List<Feature> labels = path.getLabels();
							for (Feature label : labels) {
								if (TEMPORAL_FEATURE.equalsIgnoreCase(label.getName())) {
									blocks = blockMap.get(String.format("%s-%s", label.getString(), space));
									if (blocks == null) {
										blocks = new ArrayList<String>();
										blockMap.put(String.format("%s-%s", label.getString(), space), blocks);
									}
									blocks.addAll(path.getPayload());
									break;
								}
							}
						}
					}
				}
				return blockMap;
			} else {
				// non-chronal non-spatial
				String time = getTemporalString(null);
				String space = getSpatialString(null);
				for (Path<Feature, String> path : metadataGraph.getAllPaths())
					if (path.hasPayload())
						blocks.addAll(path.getPayload());
				blockMap.put(String.format("%s-%s", time, space), blocks);
				return blockMap;
			}
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

	public List<Path<Feature, String>> query(String blockPath, GeoavailabilityQuery geoQuery) throws IOException {
		try {
			logger.info("querying filesystem " + this.name + " for block path - " + blockPath);
			List<Path<Feature, String>> featurePaths = new ArrayList<Path<Feature, String>>();
			byte[] blockBytes = Files.readAllBytes(Paths.get(blockPath));
			Block block = Serializer.deserialize(Block.class, blockBytes);
			String blockData = new String(block.getData(), "UTF-8");
			Metadata blockMeta = block.getMetadata();
			SpatialHint hint = blockMeta.getSpatialHint();
			if (hint == null) {
				logger.warning("No spatial hint present in the metadata for the block - " + blockPath);
				return featurePaths;
			}
			MetadataGraph temporaryGraph = new MetadataGraph();
			FeatureHierarchy hierarchy = metadataGraph.getFeatureHierarchy();
			int latOrder = -1;
			int longOrder = -1;
			for (String line : blockData.split("\\r?\\n")) {
				try {
					String[] features = line.split(",");
					// +2 because of the special reserved features.
					if (hierarchy.size() == features.length + 2) {
						List<Pair<String, FeatureType>> order = hierarchy.getHierarchy();
						Metadata metadata = new Metadata();
						FeatureSet featureset = new FeatureSet();
						for (int i = 0; i < features.length; i++) {
							// first two features are special reserved
							// attributes - hence i+2
							Pair<String, FeatureType> pair = order.get(i + 2);
							if (latOrder == -1 && pair.a.equalsIgnoreCase(hint.getLatitudeHint()))
								latOrder = i;
							if (longOrder == -1 && pair.a.equalsIgnoreCase(hint.getLongitudeHint()))
								longOrder = i;
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
					}
				} catch (Exception e) {
					logger.warning(e.getMessage());
				}
			}
			logger.info("Built temporary metadata graph");
			featurePaths = geoQuery.getQuery() != null ? temporaryGraph.evaluateQuery(geoQuery.getQuery())
					: temporaryGraph.getAllPaths();
			if (geoQuery.getPolygon() != null) {
				Polygon polygon = new Polygon();
				for (Coordinates coords : geoQuery.getPolygon()) {
					Point<Integer> point = GeoHash.coordinatesToXY(coords);
					polygon.addPoint(point.X(), point.Y());
				}
				int hashendIndex = blockPath.lastIndexOf(File.separator);
				String blockHash = blockPath.substring(hashendIndex - this.geohashPrecision, hashendIndex);
				logger.info("checking geohash " + blockHash + " intersection with the polygon");
				SpatialRange hashRange = GeoHash.decodeHash(blockHash);
				Pair<Coordinates, Coordinates> pair = hashRange.get2DCoordinates();
				Point<Integer> upperLeft = GeoHash.coordinatesToXY(pair.a);
				Point<Integer> lowerRight = GeoHash.coordinatesToXY(pair.b);
				if (polygon.contains(new Rectangle(upperLeft.X(), upperLeft.Y(), lowerRight.X() - upperLeft.X(),
						lowerRight.Y() - upperLeft.Y())))
					return featurePaths;
				else {
					if (latOrder != -1 && longOrder != -1) {
						GeoavailabilityMap<Path<Feature, String>> geoMap = new GeoavailabilityMap<Path<Feature, String>>(
								blockHash, GeoHash.MAX_PRECISION);
						for (Path<Feature, String> fpath : featurePaths) {
							float lat = fpath.get(latOrder).getLabel().getFloat();
							float lon = fpath.get(longOrder).getLabel().getFloat();
							if (!Float.isNaN(lat) && !Float.isNaN(lon))
								geoMap.addPoint(new Coordinates(lat, lon), fpath);
						}
						List<Path<Feature, String>> results = new ArrayList<Path<Feature, String>>();
						for (List<Path<Feature, String>> paths : geoMap.query(geoQuery).values())
							results.addAll(paths);
						logger.info("Number of paths in the considered block - " + results.size());
						return results;
					}
				}
			}
			logger.info("Number of paths in the considered block - " + featurePaths.size());
			return featurePaths;
		} catch (SerializationException | IOException | BitmapException e) {
			throw new IOException("Failed to query for the given block(" + blockPath + ") - " + e.getMessage(),
					e.getCause());
		}
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
