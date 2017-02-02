package galileo.net;

import galileo.event.Event;
import galileo.event.EventContext;

/**
 * Interface that classes should implement to to know if the client request has been completed
 * @author jcharles
 *
 */
public interface RequestListener {

	/**
	 * Called when a request is completed by the ClientRequestHandler so as to send back the response to the original client
	 * @param reponse
	 * The collective responses from all the nodes in the network
	 * @param context
	 * The context of the client that originally initiated the request
	 * @param requestHandler
	 * The request handler that is handling the client requests.
	 */
	public void onRequestCompleted(Event response, EventContext context, MessageListener requestHandler);

}
