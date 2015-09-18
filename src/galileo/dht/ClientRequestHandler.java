package galileo.dht;

import galileo.comm.GalileoEventMap;
import galileo.comm.GenericEventType;
import galileo.comm.GenericResponse;
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
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class will collect the responses from all the nodes of galileo and then
 * transfers the result to the listener. Used by the StorageNode class.
 * 
 * @author jcharles
 */
public class ClientRequestHandler implements MessageListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private GalileoEventMap eventMap;
	private BasicEventWrapper eventWrapper;
	private ClientMessageRouter router;
	private int expectedResponses;
	private List<NetworkDestination> nodes;
	private EventContext clientContext;
	private List<GalileoMessage> responses;
	private RequestListener requestListener;
	private Event response;

	public ClientRequestHandler(List<NetworkDestination> nodes,
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
		this.expectedResponses = this.nodes.size();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onMessage(GalileoMessage message) {
		if(null != message)
			this.responses.add(message);
		this.expectedResponses--;
		logger.log(Level.INFO, "Awaiting " + this.expectedResponses
				+ " more message(s)");
		if (this.expectedResponses == 0) {
			logger.log(Level.INFO, "Request successfully completed.");
			for(GalileoMessage gresponse : this.responses){
				Event event;
				try {
					event = this.eventWrapper.unwrap(gresponse);
					if(event instanceof QueryResponse && this.response instanceof QueryResponse)
						((QueryResponse)this.response).getResults().addAll(((QueryResponse)event).getResults());
					else if(event instanceof GenericResponse && this.response instanceof GenericResponse){
						GenericResponse gr = (GenericResponse)event;
						if(gr.getEventType() == GenericEventType.FEATURES && ((GenericResponse)this.response).getEventType() == GenericEventType.FEATURES)
							((Set<String>)((GenericResponse)this.response).getResults()).addAll((Set<String>)gr.getResults());
					}
				} catch (IOException | SerializationException e) {
					logger.log(Level.INFO, "An exception occurred while processing the response message. Details follow:"+e.getMessage());
				}
			}
			this.requestListener.onRequestCompleted(this.response, clientContext,
					this);
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
