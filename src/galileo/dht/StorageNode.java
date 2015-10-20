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

import galileo.bmp.Bitmap;
import galileo.bmp.BitmapVisualization;
import galileo.bmp.GeoavailabilityMap;
import galileo.bmp.GeoavailabilityQuery;
import galileo.bmp.QueryTransform;
import galileo.comm.GalileoEventMap;
import galileo.comm.GenericEvent;
import galileo.comm.GenericEventType;
import galileo.comm.GenericRequest;
import galileo.comm.GenericResponse;
import galileo.comm.GeoQueryEvent;
import galileo.comm.GeoQueryRequest;
import galileo.comm.GeoQueryResponse;
import galileo.comm.QueryEvent;
import galileo.comm.QueryRequest;
import galileo.comm.QueryResponse;
import galileo.comm.StorageEvent;
import galileo.comm.StorageRequest;
import galileo.config.SystemConfig;
import galileo.dataset.Block;
import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
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
import galileo.util.GeoHash;
import galileo.util.Version;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Primary communication component in the Galileo DHT. StorageNodes service
 * client requests and communication from other StorageNodes to disseminate
 * state information throughout the DHT.
 *
 * @author malensek
 */
public class StorageNode implements RequestListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private static final int GEO_MAP_PRECISION = 20;
	private StatusLine nodeStatus;
	
	private String hostname; //The name of this host
	private int port;
	private String rootDir;

	private File pidFile;

	private NetworkInfo network;
	private GroupInfo snGroup; //Group of this storage node in DHT

	private ServerMessageRouter messageRouter;
	private ClientConnectionPool connectionPool;
	private GeospatialFileSystem fs;
	private HashMap<String, GeoavailabilityMap<Metadata>> geoMap;

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
			if(hostname == null || hostname.length() == 0)
				throw new UnknownHostException("Failed to identify host name of the storage node. Details follow: " + e.getMessage());
		}
		this.hostname = this.hostname.toLowerCase();
		this.port = NetworkConfig.DEFAULT_PORT;
		this.rootDir = SystemConfig.getRootDir();

		// this.sessionId = HostIdentifier.getSessionId(port);
		nodeStatus = new StatusLine(SystemConfig.getRootDir() + "/status.txt");
		this.geoMap = new HashMap<String, GeoavailabilityMap<Metadata>>();
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
		network = NetworkConfig.readNetworkDescription(SystemConfig
				.getNetworkConfDir());
		List<GroupInfo> groups = network.getGroups();
		//identifying the group of this storage node
		outer: for(GroupInfo group : groups){
			List<NodeInfo> nodes = group.getNodes();
			for(NodeInfo node : nodes){
				if(node.getHostname().equalsIgnoreCase(this.hostname)){
					this.snGroup = group; //setting the group of this storage node.
					break outer;
				}
			}
		}
		if(this.snGroup == null)
			throw new Exception("Failed to identify the group of the storage node. Type 'hostname' in the terminal and make sure that it matches the hostnames specified in the network configuration files.");

		/* Set up the FileSystem. */
		nodeStatus.set("Initializing file system");
		try {
			fs = new GeospatialFileSystem(rootDir);
		} catch (FileSystemException e) {
			nodeStatus.set("File system initialization failure");
			logger.log(Level.SEVERE,
					"Could not initialize the Galileo File System!", e);
			return;
		}

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
				logger.log(
						Level.SEVERE,
						"An exception occurred while processing next event. Storage node is still up and running. Exception details follow:",
						e);
			}
		}
	}

	private void configurePartitioner() throws HashException,
			HashTopologyException, PartitionException {
		String[] geohashes = { "8g", "8u", "8v", "8x", "8y", "8z", "94", "95",
				"96", "97", "9d", "9e", "9g", "9h", "9j", "9k", "9m", "9n",
				"9p", "9q", "9r", "9s", "9t", "9u", "9v", "9w", "9x", "9y",
				"9z", "b8", "b9", "bb", "bc", "bf", "c0", "c1", "c2", "c3",
				"c4", "c6", "c8", "c9", "cb", "cc", "cd", "cf", "d4", "d5",
				"d6", "d7", "dd", "de", "dh", "dj", "dk", "dm", "dn", "dp",
				"dq", "dr", "ds", "dt", "dw", "dx", "dz", "f0", "f1", "f2",
				"f3", "f4", "f6", "f8", "f9", "fb", "fc", "fd", "ff" };

		partitioner = new SpatialHierarchyPartitioner(this, network, geohashes);
	}

	private void sendEvent(NodeInfo node, Event event) throws IOException {
		connectionPool.sendMessage(node, eventReactor.wrapEvent(event));
	}

	/**
	 * Handles a storage request from a client. This involves determining where
	 * the data belongs via a {@link Partitioner} implementation and then
	 * forwarding the data on to its destination.
	 */
	@EventHandler
	public void handleStorageRequest(StorageRequest request,
			EventContext context) throws HashException, IOException,
			PartitionException {

		/* Determine where this block goes. */
		Block file = request.getBlock();
		Metadata metadata = file.getMetadata();
		NodeInfo node = partitioner.locateData(metadata);
		logger.log(Level.INFO, "Storage destination: {0}", node);
		StorageEvent store = new StorageEvent(file);
		sendEvent(node, store);
	}

	@EventHandler
	public void handleStorage(StorageEvent store, EventContext context)
			throws FileSystemException, IOException {
		logger.log(Level.INFO, "Storing block: {0}", store.getBlock());
		Block file = store.getBlock();
		Metadata metadata = file.getMetadata();
		String geoHash = GeoHash.encode(metadata.getSpatialProperties().getCoordinates(), GEO_MAP_PRECISION);
		String basegh = geoHash.substring(0, 2);
		logger.log(Level.INFO, "Geohash of the block: {0}", basegh);
		GeoavailabilityMap<Metadata> map = this.geoMap.get(basegh);
		if(map == null){
			map = new GeoavailabilityMap<Metadata>(basegh, GEO_MAP_PRECISION);
			this.geoMap.put(basegh, map);
		}
		boolean added = map.addPoint(metadata.getSpatialProperties().getCoordinates(), metadata);
		if(added)
			logger.log(Level.INFO, "Metadata added to the GeoMap({0})", basegh);
		fs.storeBlock(store.getBlock());
	}

	/**
	 * Handles a generic request that seeks information regarding the galileo
	 * system.
	 */
	@EventHandler
	public void handleGenericRequest(GenericRequest request,
			EventContext context) throws IOException {
		if (request.getEventType() == GenericEventType.FEATURES) {
			logger.info("Generic Request: " + request.getEventType());
			GenericEvent gEvent = new GenericEvent(request.getEventType());
			ClientRequestHandler reqHandler = new ClientRequestHandler(
					network.getAllDestinations(), context, this);
			reqHandler.handleRequest(gEvent,
					new GenericResponse(request.getEventType(),
							new HashSet<String>()));
			this.requestHandlers.add(reqHandler);
		}
	}

	@EventHandler
	public void handleGenericEvent(GenericEvent genericEvent,
			EventContext context) throws IOException {
		GenericEventType eventType = genericEvent.getEventType();
		if (eventType == GenericEventType.FEATURES) {
			logger.info("Retreiving features list");
			Set<String> features = new HashSet<String>();
			features.addAll(fs.getFeaturesList());
			logger.info("Features : " + features);
			GenericResponse response = new GenericResponse(eventType, features);
			context.sendReply(response);
		}
	}

	/**
	 * Handles a query request from a client. Query requests result in a number
	 * of subqueries being performed across the Galileo network.
	 */
	@EventHandler
	public void handleQueryRequest(QueryRequest request, EventContext context)
			throws IOException {
		String queryString = request.getQueryString();
		logger.log(Level.INFO, "Query request: {0}", queryString);
		String queryId = String.valueOf(System.currentTimeMillis());
		QueryEvent qEvent = new QueryEvent(queryId, request.getQuery());
		ClientRequestHandler reqHandler = new ClientRequestHandler(
				network.getAllDestinations(), context, this);
		QueryResponse response = new QueryResponse(queryId,
				new ArrayList<Path<Feature, String>>());
		reqHandler.handleRequest(qEvent, response);
		this.requestHandlers.add(reqHandler);
	}

	/**
	 * Handles an internal Query request (from another StorageNode)
	 */
	@EventHandler
	public void handleQuery(QueryEvent query, EventContext context)
			throws IOException {
		List<Path<Feature, String>> results = new ArrayList<Path<Feature,String>>();
		try{
			logger.info(query.getQuery().toString());
			results = fs.query(query.getQuery());
		} catch(Exception e){
			logger.log(Level.SEVERE, "Something went wrong while querying the filesystem. No results obtained. Sending blank list to the client. Issue details follow:", e);
		}
		logger.info("Got " + results.size() + " results");
		QueryResponse response = new QueryResponse(query.getQueryId(), results);
		context.sendReply(response);
	}

	@EventHandler
	public void handleQueryResponse(QueryResponse response, EventContext context)
			throws IOException {
		QueryTracker tracker = queryTrackers.get(response.getId());
		if (tracker == null) {
			logger.log(Level.WARNING, "Unknown query response received: {0}",
					response.getId());
			return;
		}
	}
	
	
	/**
	 * Handles a geo query request from a client. Query requests result in a number
	 * of subqueries being performed across the Galileo network.
	 */
	@EventHandler
	public void handleGeoQueryRequest(GeoQueryRequest request, EventContext context)
			throws IOException {
		
		List<Coordinates> polygon = request.getPolygon();
		logger.log(Level.INFO, "Geoquery request: {0}", polygon);
		logger.log(Level.INFO, "Geohashes of this node: {0}", this.geoMap.keySet());
		logger.log(Level.INFO, "Geohashes of this group: {0}", this.snGroup.getGeoHashes());

		Set<GroupInfo> probableGroups = new HashSet<GroupInfo>();
		List<GroupInfo> allGroups = this.network.getGroups();
		for(Coordinates coords : polygon) {
			String geoHash = GeoHash.encode(coords, 2);
			logger.log(Level.INFO, "Geohash of the coordinates("+coords+") is " + geoHash);
			for(GroupInfo group : allGroups){
				if(group.hasGeoHash(geoHash)){
					logger.log(Level.INFO, "Probable group: "+ group.getName() +", Geohashes: " + group.getGeoHashes());
					probableGroups.add(group);
				}
			}
		}
		logger.log(Level.INFO, "Number of probable groups: " + probableGroups.size());
		Set<NetworkDestination> destinations = new HashSet<NetworkDestination>();
		for(GroupInfo group : probableGroups)
			destinations.addAll(group.getAllNodes());
		logger.log(Level.INFO, "Sending requests to: " + destinations);
		ClientRequestHandler reqHandler = new ClientRequestHandler(
				destinations, context, this);
		GeoQueryEvent gqEvent = new GeoQueryEvent(polygon);
		GeoQueryResponse response = new GeoQueryResponse(new HashSet<Metadata>());
		reqHandler.handleRequest(gqEvent, response);
		this.requestHandlers.add(reqHandler);
	}

	/**
	 * Handles an internal Geo Query request (from another StorageNode)
	 */
	@EventHandler
	public void handleGeoQueryEvent(GeoQueryEvent query, EventContext context)
			throws IOException {
		HashSet<Metadata> results = new HashSet<Metadata>();
		GeoavailabilityQuery geoQuery = new GeoavailabilityQuery(query.getPolygon());
		try{
			logger.info("Geoquery Event - Polygon:" + query.getPolygon().toString());
			String prevHash = null;
			for(Coordinates coords : query.getPolygon()) {
				String geoHash = GeoHash.encode(coords, 2);
				if(!geoHash.equalsIgnoreCase(prevHash)){
					logger.info("Geoquery Event - Coordinate:" + coords + ", GeoHash:" + geoHash);
					prevHash = geoHash;
					GeoavailabilityMap<Metadata> map = this.geoMap.get(geoHash);
					if(null != map){
						logger.info("Geoquery Event - Obtained the geoMap and issued the geoQuery");
				        BufferedImage b = BitmapVisualization.drawGeoavailabilityGrid(map.getGrid(), Color.BLACK);
				        logger.info("Geoquery Event - storing the GeoavailabilityMap image");
				        BitmapVisualization.imageToFile(b, this.hostname + "-" + geoHash + "-GeoavailabilityMap.gif");
				        Bitmap queryBitamp = QueryTransform.queryToGridBitmap(geoQuery, map.getGrid());
				        BufferedImage polyImage = BitmapVisualization.drawBitmap(queryBitamp, map.getGrid().getWidth(), map.getGrid().getHeight(), Color.RED);
				        logger.info("Geoquery Event - storing the Polygon image");
				        BitmapVisualization.imageToFile(
				                polyImage, this.hostname + "-" + geoHash + "-GeoavailabilityQuery.gif");
						Map<Integer, List<Metadata>> rMap = map.query(geoQuery);
						for(List<Metadata> mList : rMap.values()){
							logger.info("Geoquery Event Results - Size of the Metadata List: "+mList.size());
							results.addAll(mList);
						}
					}
				}
			}
		} catch(Exception e){
			logger.log(Level.SEVERE, "Something went wrong while querying the geoavailability map. Results may not be complete. Sending partial results to the client. Issue details follow:", e);
		}
		logger.info("Got " + results.size() + " results");
		GeoQueryResponse response = new GeoQueryResponse(results);
		context.sendReply(response);
	}

	/**
	 * Triggered when the request is completed by the {@link ClientRequestHandler}
	 */
	@Override
	public void onRequestCompleted(Event reponse, EventContext context,
			MessageListener requestHandler) {
		try {
			logger.info("Sending collective response to the client");
			this.requestHandlers.remove(requestHandler);
			context.sendReply(reponse);
		} catch (IOException e) {
			logger.log(Level.INFO,
					"Failed to send response to the client. Details follow: "
							+ e.getMessage());
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
