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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.comm.GalileoEventMap;
import galileo.comm.MetadataResponse;
import galileo.comm.QueryResponse;
import galileo.event.BasicEventWrapper;
import galileo.event.Event;
import galileo.event.EventContext;
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
 * @author kachikaran
 */
public class ClientRequestHandler implements MessageListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private GalileoEventMap eventMap;
	private BasicEventWrapper eventWrapper;
	private ClientMessageRouter router;
	private AtomicInteger expectedResponses;
	private Collection<NetworkDestination> nodes;
	private EventContext clientContext;
	private List<GalileoMessage> responses;
	private RequestListener requestListener;
	private Event response;
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
						List<List<String>> actualResults = actualResponse.getResults();
						List<List<String>> eventResults = eventResponse.getResults();
						actualResults.addAll(eventResults);
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
									if (actualResponse.isDryRun()) {
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
									} else {
										JSONArray actualResults = responseJSON.getJSONArray("result");
										JSONArray eventResults = eventJSON.getJSONArray("result");
										for (int i = 0; i < eventResults.length(); i++)
											actualResults.put(eventResults.getJSONObject(i));
									}
									if (responseJSON.has("hostResultSize")) {
										JSONObject aHostResultSize = responseJSON.getJSONObject("hostResultSize");
										JSONObject eHostResultSize = eventJSON.getJSONObject("hostResultSize");

										JSONObject aHostProcessingTime = responseJSON
												.getJSONObject("hostProcessingTime");
										JSONObject eHostProcessingTime = eventJSON.getJSONObject("hostProcessingTime");

										JSONObject aHostFileSize = responseJSON.getJSONObject("hostFileSize");
										JSONObject eHostFileSize = eventJSON.getJSONObject("hostFileSize");

										for (String key : eHostResultSize.keySet())
											aHostResultSize.put(key, eHostResultSize.getLong(key));
										for (String key : eHostResultSize.keySet())
											aHostProcessingTime.put(key, eHostProcessingTime.getLong(key));
										for (String key : eHostResultSize.keySet())
											aHostFileSize.put(key, eHostFileSize.getLong(key));

										responseJSON.put("totalResultSize", responseJSON.getLong("totalResultSize")
												+ eventJSON.getLong("totalResultSize"));
										responseJSON.put("totalFileSize", responseJSON.getLong("totalFileSize")
												+ eventJSON.getLong("totalFileSize"));
										responseJSON.put("totalProcessingTime",
												java.lang.Math.max(responseJSON.getLong("totalProcessingTime"),
														eventJSON.getLong("totalProcessingTime")));
										responseJSON.put("totalBlocksProcessed",
												responseJSON.getLong("totalBlocksProcessed")
														+ eventJSON.getLong("totalBlocksProcessed"));
									}
								}
							}
						}
					}
				} else if (event instanceof MetadataResponse && this.response instanceof MetadataResponse) {
					MetadataResponse emr = (MetadataResponse) event;
					JSONArray emrResults = emr.getResponse().getJSONArray("result");
					JSONObject emrJSON = emr.getResponse();
					if ("galileo#features".equalsIgnoreCase(emrJSON.getString("kind"))) {
						for (int i = 0; i < emrResults.length(); i++) {
							JSONObject fsJSON = emrResults.getJSONObject(i);
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
							this.response = new MetadataResponse(jsonResponse);
						}
					} else if ("galileo#filesystem".equalsIgnoreCase(emrJSON.getString("kind"))) {
						MetadataResponse amr = (MetadataResponse) this.response;
						JSONObject amrJSON = amr.getResponse();
						if (amrJSON.getJSONArray("result").length() == 0)
							amrJSON.put("result", emrResults);
						else {
							JSONArray amrResults = amrJSON.getJSONArray("result");
							for (int i = 0; i < emrResults.length(); i++) {
								JSONObject emrFilesystem = emrResults.getJSONObject(i);
								for (int j = 0; j < amrResults.length(); j++) {
									JSONObject amrFilesystem = amrResults.getJSONObject(j);
									if (amrFilesystem.getString("name")
											.equalsIgnoreCase(emrFilesystem.getString("name"))) {
										long latestTime = amrFilesystem.getLong("latestTime");
										long earliestTime = amrFilesystem.getLong("earliestTime");
										if (latestTime == 0 || latestTime < emrFilesystem.getLong("latestTime")) {
											amrFilesystem.put("latestTime", emrFilesystem.getLong("latestTime"));
											amrFilesystem.put("latestSpace", emrFilesystem.getString("latestSpace"));
										}
										if (earliestTime == 0 || (earliestTime > emrFilesystem.getLong("earliestTime")
												&& emrFilesystem.getLong("earliestTime") != 0)) {
											amrFilesystem.put("earliestTime", emrFilesystem.getLong("earliestTime"));
											amrFilesystem.put("earliestSpace",
													emrFilesystem.getString("earliestSpace"));
										}
										break;
									}
								}
							}
						}
					} else if ("galileo#overview".equalsIgnoreCase(emrJSON.getString("kind"))) {
						logger.info(emrJSON.getString("kind") + ": emr results length = " + emrResults.length());
						MetadataResponse amr = (MetadataResponse) this.response;
						JSONObject amrJSON = amr.getResponse();
						if (amrJSON.getJSONArray("result").length() == 0)
							amrJSON.put("result", emrResults);
						else {
							JSONArray amrResults = amrJSON.getJSONArray("result");
							for (int i = 0; i < emrResults.length(); i++) {
								JSONObject efsJSON = emrResults.getJSONObject(i);
								String efsName = efsJSON.keys().next();
								JSONObject afsJSON = null;
								for (int j = 0; j < amrResults.length(); j++) {
									if (amrResults.getJSONObject(j).has(efsName)) {
										afsJSON = amrResults.getJSONObject(j);
										break;
									}
								}
								if (afsJSON == null)
									amrResults.put(efsJSON);
								else {
									JSONArray eGeohashes = efsJSON.getJSONArray(efsName);
									JSONArray aGeohashes = afsJSON.getJSONArray(efsName);
									for (int j = 0; j < eGeohashes.length(); j++) {
										JSONObject eGeohash = eGeohashes.getJSONObject(j);
										JSONObject aGeohash = null;
										for (int k = 0; k < aGeohashes.length(); k++) {
											if (aGeohashes.getJSONObject(k).getString("region")
													.equalsIgnoreCase(eGeohash.getString("region"))) {
												aGeohash = aGeohashes.getJSONObject(k);
												break;
											}
										}
										if (aGeohash == null)
											aGeohashes.put(eGeohash);
										else {
											long eTimestamp = eGeohash.getLong("latestTimestamp");
											int blockCount = aGeohash.getInt("blockCount")
													+ eGeohash.getInt("blockCount");
											long fileSize = aGeohash.getInt("fileSize") + eGeohash.getInt("fileSize");
											aGeohash.put("blockCount", blockCount);
											aGeohash.put("fileSize", fileSize);
											if (eTimestamp > aGeohash.getLong("latestTimestamp"))
												aGeohash.put("latestTimestamp", eTimestamp);
										}
									}
								}
							}
						}
					}
				}
			} catch (IOException | SerializationException e) {
				logger.log(Level.INFO, "An exception occurred while processing the response message. Details follow:"
						+ e.getMessage());
			} catch (Exception e) {
				logger.log(Level.SEVERE,
						"An unknown exception occurred while processing the response message. Details follow:"
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
		if (awaitedResponses <= 0) {
			this.elapsedTime = System.currentTimeMillis() - this.elapsedTime;
			logger.log(Level.INFO, "Closing the request and sending back the response.");
			new Thread() {
				public void run() {
					ClientRequestHandler.this.closeRequest();
				}
			}.start();
		}
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
			this.elapsedTime = System.currentTimeMillis();
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
