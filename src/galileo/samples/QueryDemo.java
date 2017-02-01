package galileo.samples;

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

import galileo.client.EventPublisher;
import galileo.comm.GalileoEventMap;
import galileo.comm.QueryRequest;
import galileo.comm.QueryResponse;
import galileo.dataset.feature.Feature;
import galileo.event.BasicEventWrapper;
import galileo.event.EventWrapper;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Query;

public class QueryDemo implements MessageListener {

	private static GalileoEventMap eventMap = new GalileoEventMap();
	private static EventWrapper wrapper = new BasicEventWrapper(eventMap);

	@Override
	public void onConnect(NetworkDestination endpoint) {
	}

	@Override
	public void onDisconnect(NetworkDestination endpoint) {
	}

	@Override
	public void onMessage(GalileoMessage message) {
		try {
			QueryResponse response = (QueryResponse) wrapper.unwrap(message);
			System.out.println(response.getJSONResults().toString());
			/*System.out.println(response.getResults().size() + " results received");
			List<List<String>> results = response.getResults();
			for (int i = 0; i < Math.min(5, results.size()); i++) {
				System.out.println(results.get(i));
			}*/
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: galileo.samples.Query host port");
			System.exit(1);
		}
		String serverHostName = args[0];
		int serverPort = Integer.parseInt(args[1]);
		NetworkDestination server = new NetworkDestination(serverHostName, serverPort);

		QueryDemo qd = new QueryDemo();

		ClientMessageRouter messageRouter = new ClientMessageRouter();
		messageRouter.addListener(qd);

		/*
		 * Each expression chained together in an operation produces a logical
		 * AND. Each operation added to the query is like a logical OR.
		 */

		/* This query checks for total_precipitation values equal to 83.30055 */
		Query q = new Query();
		Operation o = new Operation(new Expression(">=", new Feature("ch4", 0.0f)));
		q.addOperation(o);

		System.out.println("Query 1: " + q);

		QueryRequest qr = new QueryRequest("samples", q, null);
		messageRouter.sendMessage(server, EventPublisher.wrapEvent(qr));
	}
}