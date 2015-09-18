/*
Copyright (c) 2014, Colorado State University
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

package galileo.test.dht;

import java.util.ArrayList;
import java.util.List;

import galileo.comm.DebugEvent;
import galileo.comm.GalileoEventMap;
import galileo.event.EventContext;
import galileo.event.EventHandler;
import galileo.event.EventReactor;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.NetworkDestination;
import galileo.util.PerformanceTimer; 
public class DebugClient {

    private int replies = 0;
    private ClientMessageRouter router;
    private List<NetworkDestination> dests = new ArrayList<>();
    private GalileoEventMap eventMap = new GalileoEventMap();
    private EventReactor reactor;

    public DebugClient() throws Exception {
        router = new ClientMessageRouter();
        reactor = new EventReactor(this, eventMap);
        router.addListener(reactor);

        dests.add(new NetworkDestination("lattice-0", 7555));
//        dests.add(new NetworkDestination("lattice-1", 7555));
//        dests.add(new NetworkDestination("lattice-2", 7555));
//        dests.add(new NetworkDestination("lattice-3", 7555));
//        dests.add(new NetworkDestination("lattice-4", 7555));
//        dests.add(new NetworkDestination("lattice-5", 7555));
//        dests.add(new NetworkDestination("lattice-6", 7555));
//        dests.add(new NetworkDestination("lattice-7", 7555));
//        dests.add(new NetworkDestination("lattice-8", 7555));
//        dests.add(new NetworkDestination("lattice-9", 7555));
//        dests.add(new NetworkDestination("lattice-10", 7555));
//        dests.add(new NetworkDestination("lattice-11", 7555));
//        dests.add(new NetworkDestination("lattice-12", 7555));
//        dests.add(new NetworkDestination("lattice-13", 7555));
//        dests.add(new NetworkDestination("lattice-14", 7555));
//        dests.add(new NetworkDestination("lattice-15", 7555));
//        dests.add(new NetworkDestination("lattice-16", 7555));
//        dests.add(new NetworkDestination("lattice-17", 7555));
//        dests.add(new NetworkDestination("lattice-18", 7555));
//        dests.add(new NetworkDestination("lattice-19", 7555));
//        dests.add(new NetworkDestination("lattice-20", 7555));
//        dests.add(new NetworkDestination("lattice-21", 7555));
//        dests.add(new NetworkDestination("lattice-22", 7555));
//        dests.add(new NetworkDestination("lattice-23", 7555));
//        dests.add(new NetworkDestination("lattice-24", 7555));
//        dests.add(new NetworkDestination("lattice-25", 7555));
//        dests.add(new NetworkDestination("lattice-26", 7555));
//        dests.add(new NetworkDestination("lattice-27", 7555));
//        dests.add(new NetworkDestination("lattice-28", 7555));
//        dests.add(new NetworkDestination("lattice-29", 7555));
//        dests.add(new NetworkDestination("lattice-30", 7555));
//        dests.add(new NetworkDestination("lattice-31", 7555));
//        dests.add(new NetworkDestination("lattice-32", 7555));
//        dests.add(new NetworkDestination("lattice-33", 7555));
//        dests.add(new NetworkDestination("lattice-34", 7555));
//        dests.add(new NetworkDestination("lattice-35", 7555));
//        dests.add(new NetworkDestination("lattice-36", 7555));
//        dests.add(new NetworkDestination("lattice-37", 7555));
//        dests.add(new NetworkDestination("lattice-38", 7555));
//        dests.add(new NetworkDestination("lattice-39", 7555));
//        dests.add(new NetworkDestination("lattice-40", 7555));
//        dests.add(new NetworkDestination("lattice-41", 7555));
//        dests.add(new NetworkDestination("lattice-42", 7555));
//        dests.add(new NetworkDestination("lattice-43", 7555));
//        dests.add(new NetworkDestination("lattice-44", 7555));
//        dests.add(new NetworkDestination("lattice-45", 7555));
//        dests.add(new NetworkDestination("lattice-46", 7555));
//        dests.add(new NetworkDestination("lattice-47", 7555));
//        dests.add(new NetworkDestination("lattice-48", 7555));
//        dests.add(new NetworkDestination("lattice-49", 7555));
//        dests.add(new NetworkDestination("lattice-50", 7555));
//        dests.add(new NetworkDestination("lattice-51", 7555));
//        dests.add(new NetworkDestination("lattice-52", 7555));
//        dests.add(new NetworkDestination("lattice-53", 7555));
//        dests.add(new NetworkDestination("lattice-54", 7555));
//        dests.add(new NetworkDestination("lattice-55", 7555));
//        dests.add(new NetworkDestination("lattice-56", 7555));
//        dests.add(new NetworkDestination("lattice-57", 7555));
//        dests.add(new NetworkDestination("lattice-58", 7555));
//        dests.add(new NetworkDestination("lattice-59", 7555));
//        dests.add(new NetworkDestination("lattice-60", 7555));
//        dests.add(new NetworkDestination("lattice-61", 7555));
//        dests.add(new NetworkDestination("lattice-62", 7555));
//        dests.add(new NetworkDestination("lattice-63", 7555));
//        dests.add(new NetworkDestination("lattice-64", 7555));
//        dests.add(new NetworkDestination("lattice-65", 7555));
//        dests.add(new NetworkDestination("lattice-66", 7555));
//        dests.add(new NetworkDestination("lattice-67", 7555));
//        dests.add(new NetworkDestination("lattice-68", 7555));
//        dests.add(new NetworkDestination("lattice-69", 7555));
//        dests.add(new NetworkDestination("lattice-70", 7555));
//        dests.add(new NetworkDestination("lattice-71", 7555));
//        dests.add(new NetworkDestination("lattice-72", 7555));
//        dests.add(new NetworkDestination("lattice-73", 7555));
//        dests.add(new NetworkDestination("lattice-74", 7555));
//        dests.add(new NetworkDestination("lattice-75", 7555));
//        dests.add(new NetworkDestination("lattice-76", 7555));
//        dests.add(new NetworkDestination("lattice-77", 7555));
    }

    public void start() throws Exception {
        PerformanceTimer pt = new PerformanceTimer("turnaround");
        pt.start();
        DebugEvent de = new DebugEvent(new byte[1000]);
        GalileoMessage msg = reactor.wrapEvent(de);
        router.broadcastMessage(dests, msg);

        while (true) {
            reactor.processNextEvent();
            if (replies >= dests.size()) {
                System.out.println("Complete!");
                pt.stopAndPrint();
                System.exit(0);
                return;
            }
        }
    }

    @EventHandler
    public void handleReply(DebugEvent event, EventContext context) {
        replies++;
        //System.out.println("Received reply from " + context.getSource());
    }

    public static void main(String[] args) throws Exception {
        DebugClient dc = new DebugClient();
        dc.start();
    }
}
