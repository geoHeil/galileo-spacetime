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

package galileo.client;

import java.io.IOException;

import galileo.comm.GalileoEventMap;
import galileo.event.BasicEventWrapper;
import galileo.event.Event;
import galileo.event.EventWrapper;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.NetworkDestination;

/**
 * Handles publishing events from a client to a server.
 *
 * @author malensek
 */
public class EventPublisher {

    private static GalileoEventMap eventMap = new GalileoEventMap();
    private static EventWrapper wrapper = new BasicEventWrapper(eventMap);
    private ClientMessageRouter router;

    /**
     * Creates a new EventPublisher instance using the provided
     * {@link ClientMessageRouter} instance for communications.
     */
    public EventPublisher(ClientMessageRouter router) {
        this.router = router;
    }

    /**
     * Publishes an {@link Event} via the client's {@link ClientMessageRouter}.
     */
    public void publish(NetworkDestination destination, Event event)
    throws IOException {
        GalileoMessage message = wrapper.wrap(event);
        router.sendMessage(destination, message);
    }

    /**
     * Wraps a GalileoEvent inside an EventContainer, and places the container
     * inside a GalileoMessage, ready to be transmitted across the network.
     */
    public static GalileoMessage wrapEvent(Event event)
    throws IOException {
        return wrapper.wrap(event);
    }
}
