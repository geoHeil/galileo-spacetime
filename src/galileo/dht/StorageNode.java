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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
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

import galileo.bmp.GeoavailabilityQuery;
import galileo.comm.FileSystemAction;
import galileo.comm.FileSystemEvent;
import galileo.comm.FileSystemRequest;
import galileo.comm.GalileoEventMap;
import galileo.comm.MetaEvent;
import galileo.comm.MetaRequest;
import galileo.comm.MetaResponse;
import galileo.comm.QueryEvent;
import galileo.comm.QueryRequest;
import galileo.comm.QueryResponse;
import galileo.comm.StorageEvent;
import galileo.comm.StorageRequest;
import galileo.config.SystemConfig;
import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.SpatialRange;
import galileo.dataset.feature.Feature;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashTopologyException;
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
	private int port;
	private String rootDir;
	private String resultsDir;

	private File pidFile;

	private NetworkInfo network;
	private GroupInfo snGroup; // Group of this storage node in DHT

	private ServerMessageRouter messageRouter;
	private ClientConnectionPool connectionPool;
	private Map<String, GeospatialFileSystem> fsMap;

	private GalileoEventMap eventMap = new GalileoEventMap();
	private EventReactor eventReactor = new EventReactor(this, eventMap);
	private List<ClientRequestHandler> requestHandlers;

	private Partitioner<Metadata> partitioner;

	private ConcurrentHashMap<String, QueryTracker> queryTrackers = new ConcurrentHashMap<>();

	// private String sessionId;

	public StorageNode() throws UnknownHostException {
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			this.hostname = System.getenv("HOSTNAME");
			if (hostname == null || hostname.length() == 0)
				throw new UnknownHostException(
						"Failed to identify host name of the storage node. Details follow: " + e.getMessage());
		}
		this.hostname = this.hostname.toLowerCase();
		this.port = NetworkConfig.DEFAULT_PORT;
		SystemConfig.reload();
		this.rootDir = SystemConfig.getRootDir();
		this.resultsDir = this.rootDir + "/.results";
		nodeStatus = new StatusLine(SystemConfig.getRootDir() + "/status.txt");
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

		nodeStatus.set("Setting up filesystem");
		File resultsDir = new File(this.resultsDir);
		if (!resultsDir.exists())
			resultsDir.mkdirs();

		/*
		 * Read the network configuration; if this is invalid, there is no need
		 * to execute the rest of this method.
		 */
		nodeStatus.set("Reading network configuration");
		network = NetworkConfig.readNetworkDescription(SystemConfig.getNetworkConfDir());
		List<GroupInfo> groups = network.getGroups();
		// identifying the group of this storage node
		outer: for (GroupInfo group : groups) {
			List<NodeInfo> nodes = group.getNodes();
			for (NodeInfo node : nodes) {
				if (node.getHostname().equalsIgnoreCase(this.hostname)) {
					this.snGroup = group; // setting the group of this storage
											// node.
					break outer;
				}
			}
		}
		if (this.snGroup == null)
			throw new Exception(
					"Failed to identify the group of the storage node. Type 'hostname' in the terminal and make sure that it matches the hostnames specified in the network configuration files.");

		this.fsMap = new HashMap<>();
		nodeStatus.set("Initializing communications");

		/* Set up our Shutdown hook */
		Runtime.getRuntime().addShutdownHook(new ShutdownHandler());

		/* Pre-scheduler setup tasks */
		connectionPool = new ClientConnectionPool();
		connectionPool.addListener(eventReactor);
		configurePartitioner();

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
				logger.log(Level.SEVERE,
						"An exception occurred while processing next event. Storage node is still up and running. Exception details follow:",
						e);
			}
		}
	}

	private void configurePartitioner() throws HashException, HashTopologyException, PartitionException {
		String partitionType = System.getProperty("galileo.group.partitioning", "temporal");
		if ("temporal".equalsIgnoreCase(partitionType)) {
			String temporalType = System.getProperty("galileo.group.partitioning.temporal",
					String.valueOf(Calendar.DAY_OF_MONTH));
			partitioner = new TemporalHierarchyPartitioner(this, network, Integer.parseInt(temporalType));
		} else {
			String[] geohashes = { "8g", "8u", "8v", "8x", "8y", "8z", "94", "95", "96", "97", "9d", "9e", "9g", "9h",
					"9j", "9k", "9m", "9n", "9p", "9q", "9r", "9s", "9t", "9u", "9v", "9w", "9x", "9y", "9z", "b8",
					"b9", "bb", "bc", "bf", "c0", "c1", "c2", "c3", "c4", "c6", "c8", "c9", "cb", "cc", "cd", "cf",
					"d4", "d5", "d6", "d7", "dd", "de", "dh", "dj", "dk", "dm", "dn", "dp", "dq", "dr", "ds", "dt",
					"dw", "dx", "dz", "f0", "f1", "f2", "f3", "f4", "f6", "f8", "f9", "fb", "fc", "fd", "ff" };
			partitioner = new SpatialHierarchyPartitioner(this, network, geohashes);
		}
	}

	private void sendEvent(NodeInfo node, Event event) throws IOException {
		connectionPool.sendMessage(node, eventReactor.wrapEvent(event));
	}

	@EventHandler
	public void handleFileSystemRequest(FileSystemRequest request, EventContext context)
			throws HashException, IOException, PartitionException {
		String name = request.getName();
		FileSystemAction action = request.getAction();
		List<NodeInfo> nodes = network.getAllNodes();
		FileSystemEvent event = new FileSystemEvent(name, action);
		for (NodeInfo node : nodes) {
			logger.info("Requesting " + node + " to perform a file system action");
			sendEvent(node, event);
		}
	}

	@EventHandler
	public void handleFileSystem(FileSystemEvent event, EventContext context) throws FileSystemException, IOException {
		logger.log(Level.INFO,
				"Performing action " + event.getAction().getAction() + " for file system " + event.getName());
		if (event.getAction() == FileSystemAction.CREATE) {
			GeospatialFileSystem fs = fsMap.get(event.getName());
			if (fs == null) {
				try {
					fs = new GeospatialFileSystem(rootDir, event.getName());
					fsMap.put(event.getName(), fs);
				} catch (FileSystemException | SerializationException e) {
					nodeStatus.set("File system initialization failure");
					logger.log(Level.SEVERE, "Could not initialize the Galileo File System!", e);
				}
			}
		} else if (event.getAction() == FileSystemAction.DELETE) {
			GeospatialFileSystem fs = fsMap.get(event.getName());
			if (fs != null) {
				fs.shutdown();
				fsMap.remove(event.getName());
				java.nio.file.Path directory = Paths.get(rootDir + File.separator + event.getName());
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
			}
		}
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
		Metadata metadata = file.getMetadata();
		NodeInfo node = partitioner.locateData(metadata);
		logger.log(Level.INFO, "Storage destination: {0}", node);
		StorageEvent store = new StorageEvent(file);
		sendEvent(node, store);
	}

	@EventHandler
	public void handleStorage(StorageEvent store, EventContext context) throws FileSystemException, IOException {
		String fsName = store.getBlock().getFileSystem();
		GeospatialFileSystem fs = fsMap.get(fsName);
		if (fs != null) {
			logger.log(Level.INFO, "Storing block " + store.getBlock() + " to filesystem " + fsName);
			fs.storeBlock(store.getBlock());
		} else {
			logger.log(Level.SEVERE, "Requested file system(" + fsName + ") not found. Ignoring the block.");
		}
	}

	/**
	 * Handles a meta request that seeks information regarding the galileo
	 * system.
	 */
	@EventHandler
	public void handleMetaRequest(MetaRequest request, EventContext context) throws IOException {
		try {
			logger.info("Meta Request: " + request.getRequest().getString("kind"));
			if ("galileo#filesystem".equalsIgnoreCase(request.getRequest().getString("kind"))) {
				JSONObject response = new JSONObject();
				response.put("kind", "galileo#filesystem");
				JSONArray names = new JSONArray();
				for(String fsName : fsMap.keySet()){
					GeospatialFileSystem fs = fsMap.get(fsName);
					names.put(new JSONObject().put("name", fsName).put("lastModified", fs.getLastUpdated()).put("readOnly", fs.isReadOnly()));
				}
				response.put("result", names);
				context.sendReply(new MetaResponse(response));
			} else if ("galileo#features".equalsIgnoreCase(request.getRequest().getString("kind"))) {
				JSONObject response = new JSONObject();
				response.put("kind", "galileo#filesystem");
				response.put("result", new JSONArray());
				ClientRequestHandler reqHandler = new ClientRequestHandler(network.getAllDestinations(), context, this);
				reqHandler.handleRequest(new MetaEvent(request.getRequest()), new MetaResponse(response));
				this.requestHandlers.add(reqHandler);
			} else {
				JSONObject response = new JSONObject();
				response.put("kind", request.getRequest().getString("kind"));
				response.put("error", "invalid request");
				context.sendReply(new MetaResponse(response));
			}
		} catch (Exception e) {
			JSONObject response = new JSONObject();
			String kind = "unknown";
			if (request.getRequest().has("kind"))
				kind = request.getRequest().getString("kind");
			response.put("kind", kind);
			response.put("error", e.getMessage());
			context.sendReply(new MetaResponse(response));
		}
	}

	@EventHandler
	public void handleMeta(MetaEvent event, EventContext context) throws IOException {
		if ("galileo#features".equalsIgnoreCase(event.getRequest().getString("kind"))) {
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
			context.sendReply(new MetaResponse(response));
		}
		JSONObject response = new JSONObject();
		response.put("kind", event.getRequest().getString("kind"));
		response.put("result", new JSONArray());
		context.sendReply(new MetaResponse(response));
	}

	/**
	 * Handles a query request from a client. Query requests result in a number
	 * of subqueries being performed across the Galileo network.
	 * 
	 * @throws PartitionException
	 * @throws HashException
	 */
	@EventHandler
	public void handleQueryRequest(QueryRequest request, EventContext context)
			throws IOException, HashException, PartitionException {
		String queryString = request.getQueryString();
		logger.log(Level.INFO, "Query request: {0}", queryString);
		Metadata data = new Metadata();
		if (request.isTemporal()){
			logger.log(Level.INFO, "Temporal query: {0}", request.getTemporalProperties());
			data.setTemporalProperties(request.getTemporalProperties());
		}
		if (request.isSpatial()){
			logger.log(Level.INFO, "Spatial query: {0}", request.getPolygon());
			data.setSpatialProperties(new SpatialProperties(new SpatialRange(request.getPolygon())));
		}
		List<NodeInfo> nodes = partitioner.findDestinations(data);
		logger.info("destinations: " + nodes);
		String queryId = String.valueOf(System.currentTimeMillis());
		QueryEvent qEvent = request.hasQuery()
				? new QueryEvent(queryId, request.getFileSystemName(), request.getTemporalProperties(),
						request.getPolygon(), request.getQuery(), request.isInteractive())
				: new QueryEvent(queryId, request.getFileSystemName(), request.getTemporalProperties(),
						request.getPolygon(), request.isInteractive());
		ClientRequestHandler reqHandler = new ClientRequestHandler(new ArrayList<NetworkDestination>(nodes), context,
				this);
		QueryResponse response = request.isInteractive()
				? new QueryResponse(queryId, new HashMap<String, List<Path<Feature, String>>>())
				: new QueryResponse(queryId, new JSONObject());
		reqHandler.handleRequest(qEvent, response);
		this.requestHandlers.add(reqHandler);
	}

	private String getQueryResultFileName(String queryId, String blockKey) {
		return this.resultsDir + "/" + String.format("%s-%s", blockKey, queryId) + ".json";
	}

	/**
	 * Handles an internal Query request (from another StorageNode)
	 */
	@EventHandler
	public void handleQuery(QueryEvent event, EventContext context) throws IOException {
		long resultSize = 0;
		Map<String, List<Path<Feature, String>>> results = new HashMap<String, List<Path<Feature, String>>>();
		JSONObject resultsJSON = new JSONObject();
		try {
			logger.info(event.getQueryString());
			String fsName = event.getFileSystemName();
			GeospatialFileSystem fs = fsMap.get(fsName);
			if (fs != null) {
				// results = fs.query(event);
				Metadata data = new Metadata();
				if (event.isTemporal())
					data.setTemporalProperties(event.getTemporalProperties());
				if (event.isSpatial())
					data.setSpatialProperties(new SpatialProperties(new SpatialRange(event.getPolygon())));
				Map<String, List<String>> blockMap = fs.listBlocks(data);
				System.out.println(blockMap);
				for(String blockKey: blockMap.keySet()){
					List<String> blocks = blockMap.get(blockKey);
					List<Path<Feature, String>> resultPaths = new ArrayList<Path<Feature, String>>();
					if(event.isInteractive())
						results.put(blockKey, resultPaths);
					FileWriter resultFile = null;
					if (!event.isInteractive()) {
						resultFile = new FileWriter(getQueryResultFileName(event.getQueryId(), blockKey));
						resultFile.append("[");
					}
					int blockCount = 0;
					for (String block : blocks) {
						blockCount++;
						List<Path<Feature, String>> qResults = fs.query(block,
								new GeoavailabilityQuery(event.getQuery(), event.getPolygon()));
						resultSize += qResults.size();
						if (event.isInteractive())
							resultPaths.addAll(qResults);
						else {
							int resultCount = 0;
							for (Path<Feature, String> path : qResults) {
								resultCount++;
								JSONObject jsonPath = new JSONObject();
								for (Feature feature : path.getLabels()) {
									jsonPath.put(feature.getName(), feature.getString());
								}
								resultFile.append(jsonPath.toString());
								if (blockCount != blocks.size() || resultCount != qResults.size())
									resultFile.append(",");
							}

						}
					}
					if (resultFile != null) {
						resultFile.append("]");
						resultFile.flush();
						resultFile.close();
						long fileSize = new File(getQueryResultFileName(event.getQueryId(), blockKey)).length();
						if(fileSize > 2){
							//file size of 2 indicates empty list. Including only those files having results
							JSONObject resultJSON = new JSONObject();
							resultJSON.put("filePath", getQueryResultFileName(event.getQueryId(), blockKey));
							resultJSON.put("fileSize", fileSize);
							resultJSON.put("hostName", this.hostname);
							resultsJSON.put(blockKey, new JSONArray().put(resultJSON));
						}
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
			QueryResponse response = new QueryResponse(event.getQueryId(), results);
			context.sendReply(response);
		} else {
			JSONObject responseJSON = new JSONObject();
			responseJSON.put("filesystem", event.getFileSystemName());
			responseJSON.put("queryId", event.getQueryId());
			responseJSON.put("result", resultsJSON);
			QueryResponse response = new QueryResponse(event.getQueryId(), responseJSON);
			context.sendReply(response);
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
	public void onRequestCompleted(Event reponse, EventContext context, MessageListener requestHandler) {
		try {
			logger.info("Sending collective response to the client");
			this.requestHandlers.remove(requestHandler);
			context.sendReply(reponse);
		} catch (IOException e) {
			logger.log(Level.INFO, "Failed to send response to the client. Details follow: " + e.getMessage());
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.log(Level.INFO, sw.toString());
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

			for (GeospatialFileSystem fs : fsMap.values())
				fs.shutdown();

			System.out.println("Goodbye!");
			System.exit(0);
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
