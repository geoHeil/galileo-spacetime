package galileo.comm;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.client.EventPublisher;
import galileo.event.BasicEventWrapper;
import galileo.event.Event;
import galileo.event.EventWrapper;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;

public class Connector implements MessageListener {

	private static final Logger logger = Logger.getLogger(Connector.class.getName());
	private static GalileoEventMap eventMap = new GalileoEventMap();
	private static EventWrapper wrapper = new BasicEventWrapper(eventMap);
	private ClientMessageRouter messageRouter;
	private Event response;
	private CountDownLatch latch;

	public Connector() throws IOException {
		this.messageRouter = new ClientMessageRouter();
		this.messageRouter.addListener(this);
	}

	public synchronized Event sendMessage(NetworkDestination server, Event request) throws IOException, InterruptedException {
		messageRouter.sendMessage(server, EventPublisher.wrapEvent(request));
		logger.fine("Request sent. Waiting for response");
		try {
			this.latch = new CountDownLatch(1);
			this.latch.await();
		} catch (InterruptedException e) {
			throw e;
		}
		return response;
	}
	
	public void publishEvent(NetworkDestination server, Event request) throws IOException {
		messageRouter.sendMessage(server, EventPublisher.wrapEvent(request));
	}

	@Override
	public void onMessage(GalileoMessage message) {
		try {
			logger.fine("Obtained response from Galileo");
			this.response = wrapper.unwrap(message);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "failed to get the response from Galileo", e);
		} finally {
			this.latch.countDown();
		}
	}

	public void close() {
		try {
			this.messageRouter.shutdown();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to shutdown the router", e);
		}
	}

	@Override
	public void onConnect(NetworkDestination destination) {
		logger.fine("Successfully connected to Galileo on " + destination);
	}

	@Override
	public void onDisconnect(NetworkDestination destination) {
		logger.fine("Disconnected from galileo on " + destination);
	}
}

