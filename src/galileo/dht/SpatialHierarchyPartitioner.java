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

package galileo.dht;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import galileo.dataset.Metadata;
import galileo.dataset.feature.Feature;
import galileo.dht.hash.BalancedHashRing;
import galileo.dht.hash.ConstrainedGeohash;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashRing;
import galileo.dht.hash.HashTopologyException;
import galileo.dht.hash.SHA1;

/**
 * Implements a spatial partitioner that creates a two-tiered hierarchical DHT.
 *
 * @author malensek
 */
public class SpatialHierarchyPartitioner extends Partitioner<Metadata> {

    private static final Logger logger = Logger.getLogger("galileo");

    private ConstrainedGeohash groupHash;
    private BalancedHashRing<Metadata> groupHashRing;
    private Map<BigInteger, GroupInfo> groupPositions = new HashMap<>();

    private SHA1 nodeHash = new SHA1();
    private Map<BigInteger, BalancedHashRing<byte[]>> nodeHashRings
        = new HashMap<>();
    private Map<BigInteger, Map<BigInteger, NodeInfo>> nodePositions
        = new HashMap<>();

    public SpatialHierarchyPartitioner(StorageNode storageNode,
            NetworkInfo network, String[] geohashes)
    throws PartitionException, HashException, HashTopologyException {

        super(storageNode, network);

        List<GroupInfo> groups = network.getGroups();

        if (groups.size() == 0) {
            throw new PartitionException("At least one group must exist in "
                    + "the current network configuration to use this "
                    + "partitioner.");
        }

        groupHash = new ConstrainedGeohash(geohashes);
        groupHashRing = new BalancedHashRing<>(groupHash);

        for (GroupInfo group : groups) {
            placeGroup(group);
        }
    }

    private void placeGroup(GroupInfo group)
    throws HashException, HashTopologyException {
        BigInteger position = groupHashRing.addNode(null);
        groupPositions.put(position, group);
        logger.info(String.format("Group '%s' placed at %d",
                    group.getName(), position));

        nodeHashRings.put(position, new BalancedHashRing<>(nodeHash));
        for (NodeInfo node : group.getNodes()) {
            placeNode(position, node);
        }
    }

    private void placeNode(BigInteger groupPosition, NodeInfo node)
    throws HashException, HashTopologyException {
        BalancedHashRing<byte[]> hashRing = nodeHashRings.get(groupPosition);
        BigInteger nodePosition = hashRing.addNode(null);

        GroupInfo group = groupPositions.get(groupPosition);

        logger.info(String.format("Node [%s] placed in Group '%s' at %d",
                    node, group.getName(), nodePosition));

        if (nodePositions.get(groupPosition) == null) {
            nodePositions.put(groupPosition,
                    new HashMap<BigInteger, NodeInfo>());
        }
        nodePositions.get(groupPosition).put(nodePosition, node);
    }

    @Override
    public NodeInfo locateData(Metadata metadata)
    throws HashException, PartitionException {
        /* First, determine the group that should hold this file */
        BigInteger group = groupHashRing.locate(metadata);

        /* Next, the StorageNode */
        String combinedAttrs = metadata.getName();
        for (Feature feature : metadata.getAttributes()) {
            combinedAttrs += feature.getString();
        }

        HashRing<byte[]> nodeHash = nodeHashRings.get(group);
        BigInteger node = nodeHash.locate(combinedAttrs.getBytes());
        NodeInfo info = nodePositions.get(group).get(node);
        if (info == null) {
            throw new PartitionException("Could not locate specified data");
        }
        return info;
    }
}
