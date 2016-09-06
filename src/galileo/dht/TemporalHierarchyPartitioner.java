package galileo.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.TemporalProperties;
import galileo.dht.hash.BalancedHashRing;
import galileo.dht.hash.ConstrainedGeohash;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashRing;
import galileo.dht.hash.HashTopologyException;
import galileo.dht.hash.TemporalHash;
import galileo.util.GeoHash;

public class TemporalHierarchyPartitioner extends Partitioner<Metadata> {

	private static final Logger logger = Logger.getLogger("galileo");

	private TemporalHash groupHash;
	private BalancedHashRing<Metadata> groupHashRing;
	private Map<BigInteger, GroupInfo> groupPositions;

	private ConstrainedGeohash nodeHash;
	private Map<BigInteger, BalancedHashRing<Metadata>> nodeHashRings;
	private Map<BigInteger, Map<BigInteger, NodeInfo>> nodePositions;

	public TemporalHierarchyPartitioner(StorageNode storageNode, NetworkInfo network, int temporalHashType)
			throws PartitionException, HashException, HashTopologyException {

		super(storageNode, network);

		List<GroupInfo> groups = network.getGroups();

		if (groups.size() == 0) {
			throw new PartitionException("At least one group must exist in "
					+ "the current network configuration to use this " + "partitioner.");
		}

		// Geohashes for US region.
		String[] geohashes = { "8g", "8u", "8v", "8x", "8y", "8z", "94", "95", "96", "97", "9d", "9e", "9g", "9h", "9j",
				"9k", "9m", "9n", "9p", "9q", "9r", "9s", "9t", "9u", "9v", "9w", "9x", "9y", "9z", "b8", "b9", "bb",
				"bc", "bf", "c0", "c1", "c2", "c3", "c4", "c6", "c8", "c9", "cb", "cc", "cd", "cf", "d4", "d5", "d6",
				"d7", "dd", "de", "dh", "dj", "dk", "dm", "dn", "dp", "dq", "dr", "ds", "dt", "dw", "dx", "dz", "f0",
				"f1", "f2", "f3", "f4", "f6", "f8", "f9", "fb", "fc", "fd", "ff" };

		groupHash = new TemporalHash(temporalHashType);
		groupHashRing = new BalancedHashRing<>(groupHash);
		groupPositions = new HashMap<>();
		nodeHash = new ConstrainedGeohash(geohashes);
		nodeHashRings = new HashMap<>();
		nodePositions = new HashMap<>();
		for (GroupInfo group : groups) {
			placeGroup(group);
		}
	}

	private void placeGroup(GroupInfo group) throws HashException, HashTopologyException {
		BigInteger position = groupHashRing.addNode(null);
		groupPositions.put(position, group);
		logger.info(String.format("Group '%s' placed at %d", group.getName(), position));

		nodeHashRings.put(position, new BalancedHashRing<>(nodeHash));
		for (NodeInfo node : group.getNodes()) {
			placeNode(position, node);
		}
	}

	private void placeNode(BigInteger groupPosition, NodeInfo node) throws HashException, HashTopologyException {
		BalancedHashRing<Metadata> hashRing = nodeHashRings.get(groupPosition);
		BigInteger nodePosition = hashRing.addNode(null);

		GroupInfo group = groupPositions.get(groupPosition);

		logger.info(String.format("Node [%s] placed in Group '%s' at %d", node, group.getName(), nodePosition));

		if (nodePositions.get(groupPosition) == null) {
			nodePositions.put(groupPosition, new HashMap<BigInteger, NodeInfo>());
		}
		nodePositions.get(groupPosition).put(nodePosition, node);
	}

	@Override
	public NodeInfo locateData(Metadata metadata) throws HashException, PartitionException {
		/* First, determine the group that should hold this file */
		BigInteger groupPosition = groupHashRing.locate(metadata);

		HashRing<Metadata> nodeRing = nodeHashRings.get(groupPosition);
		BigInteger node = nodeRing.locate(metadata);
		NodeInfo info = nodePositions.get(groupPosition).get(node);
		if (info == null) {
			throw new PartitionException("Could not locate specified data");
		}
		return info;
	}

	public List<NodeInfo> findDestinations(Metadata data) throws HashException {
		if (data == null)
			return network.getAllNodes();

		TemporalProperties tp = data.getTemporalProperties();
		SpatialProperties sp = data.getSpatialProperties();
		Set<NodeInfo> destinations = new HashSet<NodeInfo>();
		if (tp == null) {
			Set<BigInteger> positions = groupHashRing.getPositions();
			if (sp == null) {
				return network.getAllNodes();
			} else {
				if (sp.hasRange() && sp.getSpatialRange().hasPolygon()) {
					List<Coordinates> polygon = sp.getSpatialRange().getPolygon();
					//Spatial range
					logger.info("Polygon - " + polygon);
					// Geohash precision for spatial ring is 2.
					String[] hashes = GeoHash.getIntersectingGeohashes(polygon, 2);
					logger.info("intersecting geohashes - " + Arrays.toString(hashes));
					Metadata metadata = new Metadata();
					for (BigInteger position : positions) {
						HashRing<Metadata> nodeRing = nodeHashRings.get(position);
						for (String hash : hashes) {
							metadata.setSpatialProperties(new SpatialProperties(GeoHash.decodeHash(hash)));
							BigInteger node = nodeRing.locate(metadata);
							destinations.add(nodePositions.get(position).get(node));
						}
					}
				} else {
					for (BigInteger position : positions) {
						HashRing<Metadata> nodeRing = nodeHashRings.get(position);
						BigInteger node = nodeRing.locate(data);
						destinations.add(nodePositions.get(position).get(node));
					}
				}
			}
		} else {
			BigInteger groupPosition = groupHashRing.locate(data);
			if (sp == null) {
				HashRing<Metadata> nodeRing = nodeHashRings.get(groupPosition);
				Set<BigInteger> npositions = nodeRing.getPositions();
				for (BigInteger nposition : npositions)
					destinations.add(nodePositions.get(groupPosition).get(nposition));
			} else {
				HashRing<Metadata> nodeRing = nodeHashRings.get(groupPosition);
				if (sp.hasRange() && sp.getSpatialRange().hasPolygon()) {
					List<Coordinates> polygon = sp.getSpatialRange().getPolygon();
					String[] hashes = GeoHash.getIntersectingGeohashes(polygon, 2);
					Metadata metadata = new Metadata();
					for (String hash : hashes) {
						metadata.setSpatialProperties(new SpatialProperties(GeoHash.decodeHash(hash)));
						BigInteger node = nodeRing.locate(metadata);
						destinations.add(nodePositions.get(groupPosition).get(node));
					}
				} else {
					BigInteger node = nodeRing.locate(data);
					destinations.add(nodePositions.get(groupPosition).get(node));
				}
			}
		}
		return new ArrayList<NodeInfo>(destinations);
	}
}
