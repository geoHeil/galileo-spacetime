package galileo.dht;

import galileo.comm.GalileoEventMap;
import galileo.comm.GenericEventType;
import galileo.comm.GenericResponse;
import galileo.comm.GeoQueryResponse;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class will collect the responses from all the nodes of galileo and then
 * transfers the result to the listener. Used by the {@link StorageNode} class.
 * 
 * @author jcharles
 */
public class ClientRequestHandler implements MessageListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private static final int DELAY = 5000; // 5 seconds
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
	private boolean responding;

	public ClientRequestHandler(Collection<NetworkDestination> nodes,
			EventContext clientContext, RequestListener listener)
			throws IOException {
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
		this.responding = false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void onMessage(GalileoMessage message) {
		if (null != message)
			this.responses.add(message);
		this.timeout.set(System.currentTimeMillis() + DELAY);
		int awaitedResponses = this.expectedResponses.decrementAndGet();
		logger.log(Level.INFO, "Awaiting " + this.expectedResponses
				+ " more message(s)");
		if (awaitedResponses <= 0) {
			this.responding = true;
			logger.log(Level.INFO, "Request successfully completed.");
			for (GalileoMessage gresponse : this.responses) {
				Event event;
				try {
					event = this.eventWrapper.unwrap(gresponse);
					if (event instanceof QueryResponse
							&& this.response instanceof QueryResponse)
						((QueryResponse) this.response).getResults().addAll(
								((QueryResponse) event).getResults());
					else if (event instanceof GenericResponse
							&& this.response instanceof GenericResponse) {
						GenericResponse gr = (GenericResponse) event;
						GenericResponse thisResponse = (GenericResponse) this.response;
						if (gr.getEventType() == GenericEventType.FEATURES
								&& thisResponse.getEventType() == GenericEventType.FEATURES)
							((Set<String>) (thisResponse).getResults())
									.addAll((Set<String>) gr.getResults());
					} else if (event instanceof GeoQueryResponse 
								&& this.response instanceof GeoQueryResponse){
						GeoQueryResponse gqr = (GeoQueryResponse)event;
						GeoQueryResponse thisResponse = (GeoQueryResponse) this.response;
						thisResponse.getResults().addAll(gqr.getResults());
					}
				} catch (IOException | SerializationException e) {
					logger.log(Level.INFO,
							"An exception occurred while processing the response message. Details follow:"
									+ e.getMessage());
				}
			}
			this.requestListener.onRequestCompleted(this.response,
					clientContext, this);
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

			// Timeout thread which sets expectedResponses to zero after the
			// specified time elapses.
			this.timeout.set(System.currentTimeMillis() + DELAY);
			new Thread() {
				public void run() {
					try {
						while(System.currentTimeMillis() < timeout.get())
							Thread.sleep(500);
						if(!responding){
							logger.log(Level.INFO,
									"Request timed out. Wrapping up the request if not already closed.");
							ClientRequestHandler.this.expectedResponses.set(0);
							ClientRequestHandler.this.onMessage(null);
						}
					} catch (InterruptedException e) {
						logger.log(Level.INFO,
								"Timeout thread interrupted. Details follow: ",
								e);
					}
				};
			}.start();
		} catch (IOException e) {
			logger.log(Level.INFO,
					"Failed to send request to other nodes in the network. Details follow: "
							+ e.getMessage());
		}
	}

	@Override
	public void onConnect(NetworkDestination endpoint) {

	}

	@Override
	public void onDisconnect(NetworkDestination endpoint) {

	}
}
