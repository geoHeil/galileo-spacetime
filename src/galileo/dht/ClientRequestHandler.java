package galileo.dht;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.comm.GalileoEventMap;
import galileo.comm.MetaResponse;
import galileo.comm.QueryResponse;
import galileo.dataset.feature.Feature;
import galileo.event.BasicEventWrapper;
import galileo.event.Event;
import galileo.event.EventContext;
import galileo.graph.Path;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.net.RequestListener;
import galileo.serialization.SerializationException;

/**
 * This class will collect the responses from all the nodes of galileo and then
 * transfers the result to the listener. Used by the {@link StorageNode} class.
 * 
 * @author jkachika
 */
public class ClientRequestHandler implements MessageListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private static final int DELAY = 600000; // 600 seconds = 10 minutes
	private GalileoEventMap eventMap;
	private BasicEventWrapper eventWrapper;
	private ClientMessageRouter router;
	private AtomicInteger expectedResponses;
	private Collection<NetworkDestination> nodes;
	private EventContext clientContext;
	private List<GalileoMessage> responses;
	private RequestListener requestListener;
	private AtomicLong timeout;
	private Event response;
	private Thread timerThread;
	private long elapsedTime;

	public ClientRequestHandler(Collection<NetworkDestination> nodes, EventContext clientContext,
			RequestListener listener) throws IOException {
		this.nodes = nodes;
		this.clientContext = clientContext;
		this.requestListener = listener;

		this.router = new ClientMessageRouter(true);
		this.router.addListener(this);
		this.responses = new ArrayList<GalileoMessage>();
		this.eventMap = new GalileoEventMap();
		this.eventWrapper = new BasicEventWrapper(this.eventMap);
		this.expectedResponses = new AtomicInteger(this.nodes.size());
		this.timeout = new AtomicLong();
		this.timerThread = new Thread("Client Request Handler - Timer Thread") {
			public void run() {
				try {
					elapsedTime = System.currentTimeMillis();
					while (System.currentTimeMillis() < timeout.get()) {
						Thread.sleep(500);
						if (isInterrupted())
							throw new InterruptedException("Deliberate interruption");
					}
				} catch (InterruptedException e) {
					logger.log(Level.INFO, "Timeout thread interrupted.");
				} finally {
					logger.log(Level.INFO, "Timeout: Closing the request and sending back the response.");
					elapsedTime = System.currentTimeMillis() - elapsedTime;
					ClientRequestHandler.this.closeRequest();
				}
			};
		};
	}

	public void closeRequest() {
		silentClose(); // closing the router to make sure that no new responses
						// are added.
		class LocalFeature implements Comparable<LocalFeature> {
			String name;
			String type;
			int order;

			LocalFeature(String name, String type, int order) {
				this.name = name;
				this.type = type;
				this.order = order;
			}

			@Override
			public boolean equals(Object obj) {
				if (obj == null || !(obj instanceof LocalFeature))
					return false;
				LocalFeature other = (LocalFeature) obj;
				if (this.name.equalsIgnoreCase(other.name) && this.type.equalsIgnoreCase(other.type)
						&& this.order == other.order)
					return true;
				return false;
			}

			@Override
			public int hashCode() {
				return name.hashCode() + type.hashCode() + String.valueOf(this.order).hashCode();
			}

			@Override
			public int compareTo(LocalFeature o) {
				return this.order - o.order;
			}
		}
		Map<String, Set<LocalFeature>> resultMap = new HashMap<String, Set<LocalFeature>>();
		int responseCount = 0;
		
		for (GalileoMessage gresponse : this.responses) {
			responseCount++;
			Event event;
			try {
				event = this.eventWrapper.unwrap(gresponse);
				if (event instanceof QueryResponse && this.response instanceof QueryResponse) {
					QueryResponse actualResponse = (QueryResponse) this.response;
					actualResponse.setElapsedTime(elapsedTime);
					QueryResponse eventResponse = (QueryResponse) event;
					if (actualResponse.isInteractive() && eventResponse.isInteractive()) {
						Map<String, List<Path<Feature, String>>> actualResults = actualResponse.getResults();
						Map<String, List<Path<Feature, String>>> eventResults = eventResponse.getResults();
						for (String key : eventResults.keySet()) {
							List<Path<Feature, String>> actualPaths = actualResults.get(key);
							if (actualPaths != null)
								actualPaths.addAll(eventResults.get(key));
							else
								actualResults.put(key, eventResults.get(key));
						}
					} else {
						if (!actualResponse.isInteractive() && !eventResponse.isInteractive()) {
							JSONObject responseJSON = actualResponse.getJSONResults();
							JSONObject eventJSON = eventResponse.getJSONResults();
							if (responseJSON.length() == 0) {
								for (String name : JSONObject.getNames(eventJSON))
									responseJSON.put(name, eventJSON.get(name));
							} else {
								if (responseJSON.has("queryId") && eventJSON.has("queryId") && responseJSON
										.getString("queryId").equalsIgnoreCase(eventJSON.getString("queryId"))) {
									JSONObject actualResults = responseJSON.getJSONObject("result");
									JSONObject eventResults = eventJSON.getJSONObject("result");
									if (null != JSONObject.getNames(eventResults)) {
										for (String name : JSONObject.getNames(eventResults)) {
											if (actualResults.has(name)) {
												JSONArray ar = actualResults.getJSONArray(name);
												JSONArray er = eventResults.getJSONArray(name);
												for (int i = 0; i < er.length(); i++) {
													ar.put(er.get(i));
												}
											} else {
												actualResults.put(name, eventResults.getJSONArray(name));
											}
										}
									}
								}
							}
						}
					}
				} else if (event instanceof MetaResponse && this.response instanceof MetaResponse) {
					MetaResponse mr = (MetaResponse) event;
					JSONArray results = mr.getResponse().getJSONArray("result");
					for (int i = 0; i < results.length(); i++) {
						JSONObject fsJSON = results.getJSONObject(i);
						for (String fsName : fsJSON.keySet()) {
							Set<LocalFeature> featureSet = resultMap.get(fsName);
							if (featureSet == null) {
								featureSet = new HashSet<LocalFeature>();
								resultMap.put(fsName, featureSet);
							}
							JSONArray features = fsJSON.getJSONArray(fsName);
							for (int j = 0; j < features.length(); j++) {
								JSONObject jsonFeature = features.getJSONObject(j);
								featureSet.add(new LocalFeature(jsonFeature.getString("name"),
										jsonFeature.getString("type"), jsonFeature.getInt("order")));
							}
						}
					}
					if (this.responses.size() == responseCount) {
						JSONObject jsonResponse = new JSONObject();
						jsonResponse.put("kind", "galileo#features");
						JSONArray fsArray = new JSONArray();
						for (String fsName : resultMap.keySet()) {
							JSONObject fsJSON = new JSONObject();
							JSONArray features = new JSONArray();
							for (LocalFeature feature : new TreeSet<>(resultMap.get(fsName)))
								features.put(new JSONObject().put("name", feature.name).put("type", feature.type)
										.put("order", feature.order));
							fsJSON.put(fsName, features);
							fsArray.put(fsJSON);
						}
						jsonResponse.put("result", fsArray);
						this.response = new MetaResponse(jsonResponse);
					}
				}
			} catch (IOException | SerializationException e) {
				logger.log(Level.INFO, "An exception occurred while processing the response message. Details follow:"
						+ e.getMessage());
			} catch (Exception e) {
				logger.log(Level.SEVERE, "An unknown exception occurred while processing the response message. Details follow:"
						+ e.getMessage());
			}
		}
		this.requestListener.onRequestCompleted(this.response, clientContext, this);
	}

	@Override
	public void onMessage(GalileoMessage message) {
		if (null != message)
			this.responses.add(message);
		int awaitedResponses = this.expectedResponses.decrementAndGet();
		logger.log(Level.INFO, "Awaiting " + awaitedResponses + " more message(s)");
		if (awaitedResponses > 0) // extend timer when awaiting more responses
			this.timeout.set(System.currentTimeMillis() + DELAY);
		else
			this.timeout.set(System.currentTimeMillis()); // send response
															// immediately
	}

	/**
	 * Handles the client request on behalf of the node that received the
	 * request
	 * 
	 * @param request
	 *            - This must be a server side event: Generic Event or
	 *            QueryEvent
	 * @param response
	 */
	public void handleRequest(Event request, Event response) {
		try {
			this.response = response;
			GalileoMessage mrequest = this.eventWrapper.wrap(request);
			for (NetworkDestination node : nodes) {
				this.router.sendMessage(node, mrequest);
				logger.info("Request sent to " + node.toString());
			}

			// Timeout thread which sets expectedResponses to zero after the
			// specified time elapses.
			this.timeout.set(System.currentTimeMillis() + DELAY);
			this.timerThread.start();
		} catch (IOException e) {
			logger.log(Level.INFO,
					"Failed to send request to other nodes in the network. Details follow: " + e.getMessage());
		}
	}

	public void silentClose() {
		try {
			this.router.forceShutdown();
		} catch (Exception e) {
			logger.log(Level.INFO, "Failed to shutdown the completed client request handler: ", e);
		}
	}

	@Override
	public void onConnect(NetworkDestination endpoint) {

	}

	@Override
	public void onDisconnect(NetworkDestination endpoint) {

	}
}
