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

import galileo.dht.hash.BalancedHashRing;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashTopologyException;
import galileo.dht.hash.SHA1;

/**
 * Example Partitioner that creates a classic, balanced DHT based on file names.
 *
 * @author malensek
 */
public class SHA1Partitioner extends Partitioner<String> {

    private static final Logger logger = Logger.getLogger("galileo");

    private SHA1 hash = new SHA1();
    private BalancedHashRing<byte[]> hashRing
        = new BalancedHashRing<>(hash);

    private Map<BigInteger, NodeInfo> nodePositions = new HashMap<>();

    public SHA1Partitioner(StorageNode storageNode, NetworkInfo network)
    throws PartitionException, HashException, HashTopologyException {
        super(storageNode, network);

        List<GroupInfo> groups = network.getGroups();

        if (groups.size() == 0) {
            throw new PartitionException("One group must exist in the current "
                    + "network configuration to use the SHA1Partitioner.");
        }

        /* Fail here; if the user is expecting multiple groups or subgroups,
         * they are using the wrong partitioner. */
        if (groups.size() > 1 || groups.get(0).getGroups().size() > 0) {
            throw new PartitionException("More than one group exists in the "
                    + "current network configuration.  Only **ONE** group can "
                    + "be used with the SHA1Partitioner.");
        }

        for (NodeInfo node : groups.get(0).getNodes()) {
            placeNode(node);
        }
    }

    private void placeNode(NodeInfo node)
    throws HashException, HashTopologyException {
        BigInteger position = hashRing.addNode(null);
        nodePositions.put(position, node);
        logger.info(String.format("Node [%s] placed at %040x", node, position));
    }

    @Override
    public NodeInfo locateData(String fileName)
    throws HashException, PartitionException {
        if (fileName == null || fileName.equals("")) {
            throw new PartitionException("Cannot locate unnamed file.");
        }

        BigInteger pos = hashRing.locate(fileName.getBytes());
        return nodePositions.get(pos);
    }

	@Override
	public List<NodeInfo> findDestinations(String data) throws HashException, PartitionException {
		return network.getAllNodes();
	}
}
