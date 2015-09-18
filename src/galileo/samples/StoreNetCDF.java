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

package galileo.samples;

import java.io.IOException;

import java.util.Map;

import galileo.client.EventPublisher;
import galileo.comm.StorageRequest;
import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;

public class StoreNetCDF implements MessageListener {

    private ClientMessageRouter messageRouter;
    public EventPublisher publisher;

    public StoreNetCDF() throws IOException {
        messageRouter = new ClientMessageRouter();
        publisher = new EventPublisher(messageRouter);

        messageRouter.addListener(this);
    }

    public void disconnect() {
        messageRouter.shutdown();
    }

    @Override
    public void onConnect(NetworkDestination endpoint) {
        System.out.println("Connected to " + endpoint);
    }

    @Override
    public void onDisconnect(NetworkDestination endpoint) {
        System.out.println("Disconnected from " + endpoint);
    }

    @Override
    public void onMessage(GalileoMessage message) {
        if (message == null) {
            /* Connection was terminated */
            messageRouter.shutdown();
            return;
        }
    }

    public static void main(String[] args) throws Exception {
        String serverHostName = args[0];
        int serverPort = Integer.parseInt(args[1]);
        String fileName = args[2];

        StoreNetCDF client = new StoreNetCDF();
        NetworkDestination server
            = new NetworkDestination(serverHostName, serverPort);

        Map<String, Metadata> metas = ConvertNetCDF.readFile(fileName);
        for (Map.Entry<String, Metadata> entry : metas.entrySet()) {
            Block b = ConvertNetCDF.createBlock("", entry.getValue());
            StorageRequest store = new StorageRequest(b);
            client.publisher.publish(server, store);
        }
    }
}
