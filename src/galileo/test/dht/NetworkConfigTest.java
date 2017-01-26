package galileo.test.dht;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.Assert;

import galileo.dht.NetworkConfig;
import galileo.dht.NetworkInfo;
import galileo.dht.NodeInfo;
import galileo.net.NetworkDestination;

public class NetworkConfigTest {
	
	@Test
	public void testNetworkDestinationsOrder() throws FileNotFoundException, IOException{
		NetworkInfo niA = NetworkConfig.readNetworkDescription("config/network");
		NetworkInfo niB = NetworkConfig.readNetworkDescription("config/network");
		List<NodeInfo> niNI = niA.getAllNodes();
		List<NetworkDestination> niND = niB.getAllDestinations();
		Collections.sort(niNI);
		Collections.sort(niND);
		for(int i=0; i<niNI.size(); i++){
			System.out.println(niNI.get(i));
			System.out.println(niND.get(i));
			Assert.assertEquals(niNI.get(i), niND.get(i));
		}
	}
}
