/*
Copyright (c) 2013, Colorado State University
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

package galileo.dht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.comm.FilesystemAction;
import galileo.comm.FilesystemEvent;
import galileo.comm.FilesystemRequest;
import galileo.comm.GalileoEventMap;
import galileo.comm.MetadataEvent;
import galileo.comm.MetadataRequest;
import galileo.comm.MetadataResponse;
import galileo.comm.QueryEvent;
import galileo.comm.QueryRequest;
import galileo.comm.QueryResponse;
import galileo.comm.StorageEvent;
import galileo.comm.StorageRequest;
import galileo.comm.TemporalType;
import galileo.config.SystemConfig;
import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.SpatialRange;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashTopologyException;
import galileo.dht.hash.TemporalHash;
import galileo.event.Event;
import galileo.event.EventContext;
import galileo.event.EventHandler;
import galileo.event.EventReactor;
import galileo.fs.FileSystemException;
import galileo.fs.GeospatialFileSystem;
import galileo.graph.Path;
import galileo.net.ClientConnectionPool;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.net.PortTester;
import galileo.net.RequestListener;
import galileo.net.ServerMessageRouter;
import galileo.serialization.SerializationException;
import galileo.util.Version;

/**
 * Primary communication component in the Galileo DHT. StorageNodes service
 * client requests and communication from other StorageNodes to disseminate
 * state information throughout the DHT.
 *
 * @author malensek
 */
public class StorageNode implements RequestListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private StatusLine nodeStatus;

	private String hostname; // The name of this host
	private String canonicalHostname; // The fqdn of this host
	private int port;
	private String rootDir;
	private String resultsDir;

	private File pidFile;
	private File fsFile;

	private NetworkInfo network;

	private ServerMessageRouter messageRouter;
	private ClientConnectionPool connectionPool;
	private Map<String, GeospatialFileSystem> fsMap;

	private GalileoEventMap eventMap = new GalileoEventMap();
	private EventReactor eventReactor = new EventReactor(this, eventMap);
	private List<ClientRequestHandler> requestHandlers;

	private ConcurrentHashMap<String, QueryTracker> queryTrackers = new ConcurrentHashMap<>();

	// private String sessionId;

	public StorageNode() throws IOException {
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
			this.canonicalHostname = InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			this.hostname = System.getenv("HOSTNAME");
			if (hostname == null || hostname.length() == 0)
				throw new UnknownHostException(
						"Failed to identify host name of the storage node. Details follow: " + e.getMessage());
		}
		this.hostname = this.hostname.toLowerCase();
		this.canonicalHostname = this.canonicalHostname.toLowerCase();
		this.port = NetworkConfig.DEFAULT_PORT;
		SystemConfig.reload();
		this.rootDir = SystemConfig.getRootDir();
		this.resultsDir = this.rootDir + "/.results";
		this.nodeStatus = new StatusLine(SystemConfig.getRootDir() + "/status.txt");
		this.fsFile = new File(SystemConfig.getRootDir() + "/storage-node.fs");
		if (!this.fsFile.exists())
			this.fsFile.createNewFile();
		String pid = System.getProperty("pidFile");
		if (pid != null) {
			this.pidFile = new File(pid);
		}
		this.requestHandlers = new CopyOnWriteArrayList<ClientRequestHandler>();
	}

	/**
	 * Begins Server execution. This method attempts to fail fast to provide
	 * immediate feedback to wrapper scripts or other user interface tools. Only
	 * once all the prerequisite components are initialized and in a sane state
	 * will the StorageNode begin accepting connections.
	 */
	public void start() throws Exception {
		Version.printSplash();

		/* First, make sure the port we're binding to is available. */
		nodeStatus.set("Attempting to bind to port");
		if (PortTester.portAvailable(port) == false) {
			nodeStatus.set("Could not bind to port " + port + ".");
			throw new IOException("Could not bind to port " + port);
		}

		/*
		 * Read the network configuration; if this is invalid, there is no need
		 * to execute the rest of this method.
		 */
		nodeStatus.set("Reading network configuration");
		network = NetworkConfig.readNetworkDescription(SystemConfig.getNetworkConfDir());

		// identifying the group of this storage node
		boolean nodeFound = false;
		for (NodeInfo node : network.getAllNodes()) {
			String nodeName = node.getHostname();
			if (nodeName.equals(this.hostname) || nodeName.equals(this.canonicalHostname)) {
				nodeFound = true;
				break;
			}
		}
		if (!nodeFound)
			throw new Exception("Failed to identify the group of the storage node. "
					+ "Type 'hostname' in the terminal and make sure that it matches the "
					+ "hostnames specified in the network configuration files.");

		nodeStatus.set("Restoring filesystems");
		File resultsDir = new File(this.resultsDir);
		if (!resultsDir.exists())
			resultsDir.mkdirs();

		this.fsMap = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(fsFile))) {
			String jsonSource = br.readLine();
			if (jsonSource != null && jsonSource.length() > 0) {
				JSONObject fsJSON = new JSONObject(jsonSource);
				for (String fsName : JSONObject.getNames(fsJSON)) {
					try {
						GeospatialFileSystem gfs = GeospatialFileSystem.restoreState(this, network,
								fsJSON.getJSONObject(fsName));
						this.fsMap.put(fsName, gfs);
						logger.info("Successfully restored the filesystem - " + fsName);
					} catch (Exception e) {
						logger.log(Level.SEVERE, "could not restore filesystem - " + fsName, e);
					}
				}
			}
		} catch (IOException ioe) {
			logger.log(Level.SEVERE, "Failed to restore filesystems", ioe);
		}

		/* Set up our Shutdown hook */
		Runtime.getRuntime().addShutdownHook(new ShutdownHandler());

		/* Pre-scheduler setup tasks */
		connectionPool = new ClientConnectionPool();
		connectionPool.addListener(eventReactor);

		/* Start listening for incoming messages. */
		messageRouter = new ServerMessageRouter();
		messageRouter.addListener(eventReactor);
		messageRouter.listen(port);
		nodeStatus.set("Online");

		/* Start processing the message loop */
		while (true) {
			try {
				eventReactor.processNextEvent();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "An exception occurred while processing next event. "
						+ "Storage node is still up and running. Exception details follow:", e);
			}
		}
	}

	private void sendEvent(NodeInfo node, Event event) throws IOException {
		connectionPool.sendMessage(node, eventReactor.wrapEvent(event));
	}

	@EventHandler
	public void handleFileSystemRequest(FilesystemRequest request, EventContext context)
			throws HashException, IOException, PartitionException {
		String name = request.getName();
		FilesystemAction action = request.getAction();
		List<NodeInfo> nodes = network.getAllNodes();
		FilesystemEvent event = new FilesystemEvent(name, action, request.getFeatureList(), request.getSpatialHint());
		event.setPrecision(request.getPrecision());
		event.setNodesPerGroup(request.getNodesPerGroup());
		event.setTemporalType(request.getTemporalType());
		for (NodeInfo node : nodes) {
			logger.info("Requesting " + node + " to perform a file system action");
			sendEvent(node, event);
		}
	}

	@EventHandler
	public void handleFileSystem(FilesystemEvent event, EventContext context) {
		logger.log(Level.INFO,
				"Performing action " + event.getAction().getAction() + " for file system " + event.getName());
		if (event.getAction() == FilesystemAction.CREATE) {
			GeospatialFileSystem fs = fsMap.get(event.getName());
			if (fs == null) {
				try {
					fs = new GeospatialFileSystem(this, this.rootDir, event.getName(), event.getPrecision(),
							event.getNodesPerGroup(), event.getTemporalValue(), this.network, event.getFeatures(),
							event.getSpatialHint(), false);
					fsMap.put(event.getName(), fs);
				} catch (FileSystemException | SerializationException | IOException | PartitionException | HashException
						| HashTopologyException e) {
					logger.log(Level.SEVERE, "Could not initialize the Galileo File System!", e);
				}
			}
		} else if (event.getAction() == FilesystemAction.DELETE) {
			GeospatialFileSystem fs = fsMap.get(event.getName());
			if (fs != null) {
				fs.shutdown();
				fsMap.remove(event.getName());
				java.nio.file.Path directory = Paths.get(rootDir + File.separator + event.getName());
				try {
					Files.walkFileTree(directory, new SimpleFileVisitor<java.nio.file.Path>() {
						@Override
						public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
								throws IOException {
							Files.delete(file);
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException exc)
								throws IOException {
							Files.delete(dir);
							return FileVisitResult.CONTINUE;
						}
					});
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Failed to delete the requested file System!", e);
				}
			}
		}
		persistFilesystems();
	}

	/**
	 * Handles a storage request from a client. This involves determining where
	 * the data belongs via a {@link Partitioner} implementation and then
	 * forwarding the data on to its destination.
	 */
	@EventHandler
	public void handleStorageRequest(StorageRequest request, EventContext context)
			throws HashException, IOException, PartitionException {
		/* Determine where this block goes. */
		Block file = request.getBlock();
		String fsName = file.getFilesystem();
		if (fsName != null) {
			GeospatialFileSystem gfs = this.fsMap.get(fsName);
			if (gfs != null) {
				Metadata metadata = file.getMetadata();
				Partitioner<Metadata> partitioner = gfs.getPartitioner();
				NodeInfo node = partitioner.locateData(metadata);
				logger.log(Level.INFO, "Storage destination: {0}", node);
				StorageEvent store = new StorageEvent(file);
				sendEvent(node, store);
			} else {
				logger.log(Level.WARNING, "No filesystem found for the specified name " + fsName + ". Request ignored");
			}
		} else {
			logger.log(Level.WARNING, "No filesystem name specified to store the block. Request ignored");
		}
	}

	@EventHandler
	public void handleStorage(StorageEvent store, EventContext context) {
		String fsName = store.getBlock().getFilesystem();
		GeospatialFileSystem fs = fsMap.get(fsName);
		if (fs != null) {
			logger.log(Level.INFO, "Storing block " + store.getBlock() + " to filesystem " + fsName);
			try {
				fs.storeBlock(store.getBlock());
			} catch (FileSystemException | IOException e) {
				logger.log(Level.SEVERE, "Something went wrong while storing the block.", e);
			}
		} else {
			logger.log(Level.SEVERE, "Requested file system(" + fsName + ") not found. Ignoring the block.");
		}
	}

	/**
	 * Handles a meta request that seeks information regarding the galileo
	 * system.
	 */
	@EventHandler
	public void handleMetadataRequest(MetadataRequest request, EventContext context) {
		try {
			logger.info("Meta Request: " + request.getRequest().getString("kind"));
			if ("galileo#filesystem".equalsIgnoreCase(request.getRequest().getString("kind"))) {
				JSONObject response = new JSONObject();
				response.put("kind", "galileo#filesystem");
				response.put("result", new JSONArray());
				ClientRequestHandler reqHandler = new ClientRequestHandler(network.getAllDestinations(), context, this);
				reqHandler.handleRequest(new MetadataEvent(request.getRequest()), new MetadataResponse(response));
				this.requestHandlers.add(reqHandler);
			} else if ("galileo#features".equalsIgnoreCase(request.getRequest().getString("kind"))) {
				JSONObject response = new JSONObject();
				response.put("kind", "galileo#features");
				response.put("result", new JSONArray());
				ClientRequestHandler reqHandler = new ClientRequestHandler(network.getAllDestinations(), context, this);
				reqHandler.handleRequest(new MetadataEvent(request.getRequest()), new MetadataResponse(response));
				this.requestHandlers.add(reqHandler);
			} else if ("galileo#overview".equalsIgnoreCase(request.getRequest().getString("kind"))) {
				JSONObject response = new JSONObject();
				response.put("kind", "galileo#overview");
				response.put("result", new JSONArray());
				ClientRequestHandler reqHandler = new ClientRequestHandler(network.getAllDestinations(), context, this);
				reqHandler.handleRequest(new MetadataEvent(request.getRequest()), new MetadataResponse(response));
				this.requestHandlers.add(reqHandler);
			} else {
				JSONObject response = new JSONObject();
				response.put("kind", request.getRequest().getString("kind"));
				response.put("error", "invalid request");
				context.sendReply(new MetadataResponse(response));
			}
		} catch (Exception e) {
			JSONObject response = new JSONObject();
			String kind = "unknown";
			if (request.getRequest().has("kind"))
				kind = request.getRequest().getString("kind");
			response.put("kind", kind);
			response.put("error", e.getMessage());
			try {
				context.sendReply(new MetadataResponse(response));
			} catch (IOException e1) {
				logger.log(Level.SEVERE, "Failed to send response to the original client", e);
			}
		}
	}

	@EventHandler
	public void handleMetadata(MetadataEvent event, EventContext context) throws IOException {
		if ("galileo#filesystem".equalsIgnoreCase(event.getRequest().getString("kind"))) {
			JSONObject response = new JSONObject();
			response.put("kind", "galileo#filesystem");
			JSONArray result = new JSONArray();
			for (String fsName : fsMap.keySet()) {
				GeospatialFileSystem fs = fsMap.get(fsName);
				result.put(fs.obtainState());
			}
			response.put("result", result);
			context.sendReply(new MetadataResponse(response));
		} else if ("galileo#overview".equalsIgnoreCase(event.getRequest().getString("kind"))) {
			JSONObject request = event.getRequest();
			JSONObject response = new JSONObject();
			response.put("kind", "galileo#overview");
			JSONArray result = new JSONArray();
			if (request.has("filesystem") && request.get("filesystem") instanceof JSONArray) {
				JSONArray fsNames = request.getJSONArray("filesystem");
				for (int i = 0; i < fsNames.length(); i++) {
					GeospatialFileSystem fs = fsMap.get(fsNames.getString(i));
					if (fs != null) {
						JSONArray overview = fs.getOverview();
						JSONObject fsOverview = new JSONObject();
						fsOverview.put(fsNames.getString(i), overview);
						result.put(fsOverview);
					} else {
						JSONObject fsOverview = new JSONObject();
						fsOverview.put(fsNames.getString(i), new JSONArray());
						result.put(fsOverview);
					}
				}
			}
			response.put("result", result);
			logger.info(response.toString());
			context.sendReply(new MetadataResponse(response));
		} else if ("galileo#features".equalsIgnoreCase(event.getRequest().getString("kind"))) {
			JSONObject request = event.getRequest();
			JSONObject response = new JSONObject();
			response.put("kind", "galileo#features");
			JSONArray result = new JSONArray();
			if (request.has("filesystem") && request.get("filesystem") instanceof JSONArray) {
				JSONArray fsNames = request.getJSONArray("filesystem");
				for (int i = 0; i < fsNames.length(); i++) {
					GeospatialFileSystem fs = fsMap.get(fsNames.getString(i));
					if (fs != null) {
						JSONArray features = fs.getFeaturesJSON();
						JSONObject fsFeatures = new JSONObject();
						fsFeatures.put(fsNames.getString(i), features);
						result.put(fsFeatures);
					} else {
						JSONObject fsFeatures = new JSONObject();
						fsFeatures.put(fsNames.getString(i), new JSONArray());
						result.put(fsFeatures);
					}
				}
			} else {
				for (String fsName : fsMap.keySet()) {
					GeospatialFileSystem fs = fsMap.get(fsName);
					if (fs != null) {
						JSONArray features = fs.getFeaturesJSON();
						JSONObject fsFeatures = new JSONObject();
						fsFeatures.put(fsName, features);
						result.put(fsFeatures);
					} else {
						JSONObject fsFeatures = new JSONObject();
						fsFeatures.put(fsName, new JSONArray());
						result.put(fsFeatures);
					}
				}
			}
			response.put("result", result);
			context.sendReply(new MetadataResponse(response));
		} else {
			JSONObject response = new JSONObject();
			response.put("kind", event.getRequest().getString("kind"));
			response.put("result", new JSONArray());
			context.sendReply(new MetadataResponse(response));
		}
	}

	/**
	 * Handles a query request from a client. Query requests result in a number
	 * of subqueries being performed across the Galileo network.
	 * 
	 * @throws PartitionException
	 * @throws HashException
	 */
	@EventHandler
	public void handleQueryRequest(QueryRequest request, EventContext context) {
		String featureQueryString = request.getFeatureQueryString();
		String metadataQueryString = request.getMetadataQueryString();
		logger.log(Level.INFO, "Feature query request: {0}", featureQueryString);
		logger.log(Level.INFO, "Metadata query request: {0}", metadataQueryString);
		String queryId = String.valueOf(System.currentTimeMillis());
		GeospatialFileSystem gfs = this.fsMap.get(request.getFilesystemName());
		QueryResponse response = request.isInteractive()
				? new QueryResponse(queryId, gfs.getFeaturesList(), new ArrayList<List<String>>())
				: new QueryResponse(queryId, new JSONObject());
		if (gfs != null) {
			Metadata data = new Metadata();
			if (request.isTemporal()) {
				String[] timeSplit = request.getTime().split("-");
				int timeIndex = Arrays.asList(TemporalType.values()).indexOf(gfs.getTemporalType());
				if (!timeSplit[timeIndex].contains("x")) {
					logger.log(Level.INFO, "Temporal query: {0}", request.getTime());
					Calendar c = Calendar.getInstance();
					c.setTimeZone(TemporalHash.TIMEZONE);
					int year = timeSplit[0].charAt(0) == 'x' ? c.get(Calendar.YEAR) : Integer.parseInt(timeSplit[0]);
					int month = timeSplit[1].charAt(0) == 'x' ? c.get(Calendar.MONTH)
							: Integer.parseInt(timeSplit[1]) - 1;
					int day = timeSplit[2].charAt(0) == 'x' ? c.get(Calendar.DAY_OF_MONTH)
							: Integer.parseInt(timeSplit[2]);
					int hour = timeSplit[3].charAt(0) == 'x' ? c.get(Calendar.HOUR_OF_DAY)
							: Integer.parseInt(timeSplit[3]);
					c.set(year, month, day, hour, 0);
					data.setTemporalProperties(new TemporalProperties(c.getTimeInMillis()));
				}
			}
			if (request.isSpatial()) {
				logger.log(Level.INFO, "Spatial query: {0}", request.getPolygon());
				data.setSpatialProperties(new SpatialProperties(new SpatialRange(request.getPolygon())));
			}
			Partitioner<Metadata> partitioner = gfs.getPartitioner();
			List<NodeInfo> nodes;
			try {
				nodes = partitioner.findDestinations(data);
				logger.info("destinations: " + nodes);
				QueryEvent qEvent = (request.hasFeatureQuery() || request.hasMetadataQuery())
						? new QueryEvent(queryId, request.getFilesystemName(), request.getFeatureQuery(),
								request.getMetadataQuery())
						: (request.isSpatial())
								? new QueryEvent(queryId, request.getFilesystemName(), request.getPolygon())
								: new QueryEvent(queryId, request.getFilesystemName(), request.getTime());

				if (request.isDryRun()) {
					qEvent.enableDryRun();
					response.setDryRun(true);
				}
				if (request.isInteractive())
					qEvent.makeInteractive();
				if (request.isSpatial())
					qEvent.setPolygon(request.getPolygon());
				if (request.isTemporal())
					qEvent.setTime(request.getTime());

				try {
					ClientRequestHandler reqHandler = new ClientRequestHandler(new ArrayList<NetworkDestination>(nodes),
							context, this);
					reqHandler.handleRequest(qEvent, response);
					this.requestHandlers.add(reqHandler);
				} catch (IOException ioe) {
					logger.log(Level.SEVERE,
							"Failed to initialize a ClientRequestHandler. Sending unfinished response back to client",
							ioe);
					try {
						context.sendReply(response);
					} catch (IOException e) {
						logger.log(Level.SEVERE, "Failed to send response back to original client", e);
					}
				}
			} catch (HashException | PartitionException hepe) {
				logger.log(Level.SEVERE,
						"Failed to identify the destination nodes. Sending unfinished response back to client", hepe);
				try {
					context.sendReply(response);
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Failed to send response back to original client", e);
				}
			}
		} else {
			try {
				context.sendReply(response);
			} catch (IOException ioe) {
				logger.log(Level.SEVERE, "Failed to send response back to original client", ioe);
			}
		}
	}

	private String getQueryResultFileName(String queryId, String fsName) {
		return this.resultsDir + "/" + String.format("%s-%s", fsName, queryId) + ".json";
	}

	/**
	 * Handles an internal Query request (from another StorageNode)
	 */
	@EventHandler
	public void handleQuery(QueryEvent event, EventContext context) {
		long resultSize = 0;
		long hostFileSize = 0;
		long totalProcessingTime = 0;
		long blocksProcessed = 0;
		long minimumResultSize = 2;
		List<List<String>> resultPaths = new ArrayList<List<String>>();
		List<String> resultHeader = new ArrayList<String>();
		JSONObject blocksJSON = new JSONObject();
		JSONArray resultsJSON = new JSONArray();
		long processingTime = System.currentTimeMillis();
		try {
			logger.info(event.getFeatureQueryString());
			logger.info(event.getMetadataQueryString());
			String fsName = event.getFilesystemName();
			GeospatialFileSystem fs = fsMap.get(fsName);
			if (fs != null) {
				resultHeader.addAll(fs.getFeaturesList());
				Map<String, List<String>> blockMap = fs.listBlocks(event.getTime(), event.getPolygon(),
						event.getMetadataQuery(), event.isDryRun());
				if (event.isDryRun()) {
					JSONObject responseJSON = new JSONObject();
					responseJSON.put("filesystem", event.getFilesystemName());
					responseJSON.put("queryId", event.getQueryId());
					for (String blockKey : blockMap.keySet()) {
						blocksJSON.put(blockKey, new JSONArray(blockMap.get(blockKey)));
					}
					responseJSON.put("result", blocksJSON);
					QueryResponse response = new QueryResponse(event.getQueryId(), responseJSON);
					context.sendReply(response);
					return;
				}
				FileWriter resultFile = null;
				try {
					if (!event.isInteractive()) {
						resultFile = new FileWriter(getQueryResultFileName(event.getQueryId(), fsName));
						resultFile.append("[");
						String header = fs.getFeaturesRepresentation().toString();
						resultFile.append(header);
						minimumResultSize += header.length();
					}

					List<Path<Feature, String>> qResults = new ArrayList<>();
					for (String blockKey : blockMap.keySet()) {
						List<String> blocks = blockMap.get(blockKey);
						for (int i = 1; i <= blocks.size(); i++) {
							String block = blocks.get(i - 1);
							blocksProcessed++;
							qResults.addAll(fs.query(block, event.getFeatureQuery()));
							if (qResults.size() > GeospatialFileSystem.MAX_GRID_POINTS || i == blocks.size()) {
								qResults = fs.query(blockKey, qResults, event.getPolygon());
								resultSize += qResults.size();
								if (event.isInteractive()) {
									for (Path<Feature, String> path : qResults) {
										List<String> pathValues = new ArrayList<String>();
										for (Feature feature : path.getLabels())
											pathValues.add(feature.getString());
										resultPaths.add(pathValues);
									}
								} else {
									for (Path<Feature, String> path : qResults) {
										JSONArray jsonPath = new JSONArray();
										for (Feature feature : path.getLabels())
											jsonPath.put(feature.getString());
										resultFile.append(",\n" + jsonPath.toString());
									}
								}
								qResults = new ArrayList<>();
							}

						}
					}
					if (resultFile != null) {
						resultFile.append("]");
						resultFile.flush();
						resultFile.close();
						totalProcessingTime = System.currentTimeMillis() - processingTime;
						long fileSize = new File(getQueryResultFileName(event.getQueryId(), fsName)).length();
						if (fileSize > minimumResultSize) {
							hostFileSize += fileSize;
							// file size of minimumResultSize indicates empty
							// list. Including only those files having results
							JSONObject resultJSON = new JSONObject();
							resultJSON.put("filePath", getQueryResultFileName(event.getQueryId(), fsName));
							resultJSON.put("fileSize", fileSize);
							resultJSON.put("hostName", this.canonicalHostname);
							resultJSON.put("resultSize", resultSize);
							resultJSON.put("processingTime", totalProcessingTime);
							resultsJSON.put(resultJSON);
						}
					}
				} finally {
					try {
						if (resultFile != null)
							resultFile.close();
					} catch (IOException ex) {
						logger.log(Level.SEVERE, "Failed to close the results file - "
								+ getQueryResultFileName(event.getQueryId(), fsName), ex);
					}
				}
			} else {
				logger.log(Level.SEVERE, "Requested file system(" + fsName
						+ ") not found. Ignoring the query and returning empty results.");
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE,
					"Something went wrong while querying the filesystem. No results obtained. Sending blank list to the client. Issue details follow:",
					e);
		}
		logger.info("Got " + resultSize + " results");
		if (event.isInteractive()) {
			QueryResponse response = new QueryResponse(event.getQueryId(), resultHeader, resultPaths);
			response.setDryRun(event.isDryRun());
			try {
				context.sendReply(response);
			} catch (IOException ioe) {
				logger.log(Level.SEVERE, "Failed to send response back to ClientRequestHandler", ioe);
			}
		} else {
			JSONObject responseJSON = new JSONObject();
			responseJSON.put("filesystem", event.getFilesystemName());
			responseJSON.put("queryId", event.getQueryId());
			responseJSON.put("result", resultsJSON);
			responseJSON.put("hostResultSize", new JSONObject().put(this.canonicalHostname, resultSize));
			responseJSON.put("totalResultSize", resultSize);
			responseJSON.put("hostFileSize", new JSONObject().put(this.canonicalHostname, hostFileSize));
			responseJSON.put("totalFileSize", hostFileSize);
			responseJSON.put("hostProcessingTime", new JSONObject().put(this.canonicalHostname, totalProcessingTime));
			responseJSON.put("totalProcessingTime", totalProcessingTime);
			responseJSON.put("totalBlocksProcessed", blocksProcessed);
			QueryResponse response = new QueryResponse(event.getQueryId(), responseJSON);
			response.setDryRun(event.isDryRun());
			try {
				context.sendReply(response);
			} catch (IOException ioe) {
				logger.log(Level.SEVERE, "Failed to send response back to ClientRequestHandler", ioe);
			}
		}
	}

	@EventHandler
	public void handleQueryResponse(QueryResponse response, EventContext context) throws IOException {
		QueryTracker tracker = queryTrackers.get(response.getId());
		if (tracker == null) {
			logger.log(Level.WARNING, "Unknown query response received: {0}", response.getId());
			return;
		}
	}

	/**
	 * Triggered when the request is completed by the
	 * {@link ClientRequestHandler}
	 */
	@Override
	public void onRequestCompleted(Event response, EventContext context, MessageListener requestHandler) {
		try {
			logger.info("Sending collective response to the client");
			this.requestHandlers.remove(requestHandler);
			context.sendReply(response);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed to send response to the client.", e);
		} finally {
			System.gc();
		}
	}

	public void persistFilesystems() {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(fsFile))) {
			JSONObject fsJSON = new JSONObject();
			for (String fsName : fsMap.keySet()) {
				GeospatialFileSystem fs = fsMap.get(fsName);
				fsJSON.put(fsName, fs.obtainState());
			}
			bw.write(fsJSON.toString());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * Handles cleaning up the system for a graceful shutdown.
	 */
	private class ShutdownHandler extends Thread {
		@Override
		public void run() {
			/*
			 * The logging subsystem may have already shut down, so we revert to
			 * stdout for our final messages
			 */
			System.out.println("Initiated shutdown.");

			try {
				connectionPool.forceShutdown();
				messageRouter.shutdown();
			} catch (Exception e) {
				e.printStackTrace();
			}

			nodeStatus.close();

			if (pidFile != null && pidFile.exists()) {
				pidFile.delete();
			}

			persistFilesystems();

			for (GeospatialFileSystem fs : fsMap.values())
				fs.shutdown();

			System.out.println("Goodbye!");
		}
	}

	/**
	 * Executable entrypoint for a Galileo DHT Storage Node
	 */
	public static void main(String[] args) {
		try {
			StorageNode node = new StorageNode();
			node.start();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not start StorageNode.", e);
		}
	}
}
