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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import galileo.client.EventPublisher;
import galileo.comm.GalileoEventMap;
import galileo.comm.QueryEvent;
import galileo.comm.QueryResponse;
import galileo.comm.StorageRequest;
import galileo.dataset.feature.Feature;
import galileo.event.BasicEventWrapper;
import galileo.event.EventWrapper;
import galileo.graph.Path;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Operator;
import galileo.query.Query;
import galileo.util.PerformanceTimer;

public class RandomQuery implements MessageListener, Runnable {

	private static GalileoEventMap eventMap = new GalileoEventMap();
	private static EventWrapper wrapper = new BasicEventWrapper(eventMap);

	public static int clients;
	private static int storageOps = 1;
	private static boolean noNotEqual = true;
	private static boolean reverseBigRanges = true;

	public static Random random = new Random();

	public static Operator randomOperator() {
		int i = random.nextInt(Operator.values().length - 2) + 1;
		return Operator.fromInt(i);
	}

	public static Feature randomFeature(String name, float min, float max) {
		float diff = max - min;
		float rand = random.nextFloat() * diff;
		float value = min + rand;
		return new Feature(name, value);
	}

	public static Query randomQuery() {
		Query q = new Query();

		List<Feature> features = new ArrayList<>();
		features.add(randomFeature("humidity", 1, 100));
		features.add(randomFeature("wind_direction", 1, 100));
		features.add(randomFeature("condensation", 1, 100));
		features.add(randomFeature("temperature", 1, 100));

		List<Expression> expressions = new ArrayList<>();
		for (Feature f : features) {
			Operator op = randomOperator();
			if (noNotEqual == true && op == Operator.NOTEQUAL) {
				op = Operator.EQUAL;
			}
			if (reverseBigRanges == true) {
				if ((op == Operator.GREATER || op == Operator.GREATEREQUAL) && f.getFloat() <= 35.0f) {
					op = Operator.LESS;
				}
				if ((op == Operator.LESS || op == Operator.LESSEQUAL) && f.getFloat() >= 65.0f) {
					op = Operator.GREATER;
				}
			}
			expressions.add(new Expression(op, f));
		}

		Operation op = new Operation(expressions.toArray(new Expression[expressions.size()]));

		q.addOperation(op);

		return q;
	}

	/*--------------------------------------------------------------------*/

	private ClientMessageRouter messageRouter;
	private NetworkDestination server;
	private PerformanceTimer resp = new PerformanceTimer("ResponseTime");
	private boolean responded;

	public RandomQuery(String serverHostName, int serverPort) throws Exception {
		clients++;

		messageRouter = new ClientMessageRouter();
		messageRouter.addListener(this);
		server = new NetworkDestination(serverHostName, serverPort);

		/* Sleep for a random amount of time */
		Thread.sleep(random.nextInt(3000));
	}

	public void run() {
		while (true) {
			try {
				send();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void send() throws Exception {
		byte[] id = new byte[1024];
		random.nextBytes(id);
		BigInteger bi = new BigInteger(id);

		Query q = randomQuery();
		System.out.println(q);
		QueryEvent qe = new QueryEvent(bi.toString(16), "samples", q);

		resp.start();
		messageRouter.sendMessage(server, EventPublisher.wrapEvent(qe));
		responded = false;

		/* Sleep for ~1s. */
		Thread.sleep(1000);
		while (responded == false) {
			Thread.sleep(100);
		}

		for (int s = 0; s < storageOps; ++s) {
			messageRouter.sendMessage(server,
					EventPublisher.wrapEvent(new StorageRequest(RandomBlocks.generateData())));
		}
	}

	@Override
	public void onConnect(NetworkDestination endpoint) {
	}

	@Override
	public void onDisconnect(NetworkDestination endpoint) {
		System.out.println("Disconnected from the server.  Goodbye!");
		System.exit(0);
	}

	@Override
	public void onMessage(GalileoMessage message) {
		resp.stop();
		System.out.println(clients + "    " + resp.getLastResult());
		try {
			QueryResponse response = (QueryResponse) wrapper.unwrap(message);
			System.out.println(response.getResults().size() + " results received");

			Map<String, List<Path<Feature, String>>> results = response.getResults();
			for (String key : results.keySet()) {
				System.out.println("First 5 results of " + key + ":");
				int limit = 5;
				if (results.size() < 5) {
					limit = results.size();
				}
				int counter = 0;
				for (Path<Feature, String> path : results.get(key)) {
					if (path.size() > 1) {
						System.out.println(path);
						counter++;
					}
					if (counter == limit) {
						break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		responded = true;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: galileo.samples.RandomQuery host port");
			System.exit(1);
		}
		String serverHostName = args[0];
		int serverPort = Integer.parseInt(args[1]);

		int threads = 1;
		if (args.length >= 3) {
			threads = Integer.parseInt(args[2]);
		}

		for (int t = 0; t < threads; ++t) {
			RandomQuery rq = new RandomQuery(serverHostName, serverPort);
			new Thread(rq).start();
		}
	}
}
