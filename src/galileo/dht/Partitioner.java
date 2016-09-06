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

import java.util.List;

import galileo.dht.hash.HashException;

/**
 * This provides an abstract implementation of a Galileo Partitioner, which
 * determines where all information is distributed in the system.
 *
 * @author malensek
 */
public abstract class Partitioner<T> {

	protected StorageNode storageNode;
	protected NetworkInfo network;

	public Partitioner(StorageNode storageNode, NetworkInfo network) {
		this.storageNode = storageNode;
		this.network = network;
	}

	/**
	 * Determines where a file belongs in the system based on its properties.
	 * This function could implement a simple hash-based partitioning scheme,
	 * something more dynamic, utilize the feature graph, etc.
	 *
	 * Ultimately, this function will determine the DHT hierarchy.
	 *
	 * @param data
	 *            data to find the location in the network for.
	 */
	public abstract NodeInfo locateData(T data) throws HashException, PartitionException;

	/**
	 * Determines the nodes that contains the information based on the
	 * properties of the data,
	 * 
	 * @param data
	 *            data to find the nodes responsible for it.
	 * @return List of NodeInfo objects
	 * @throws HashException
	 * @throws PartitionException
	 */
	public abstract List<NodeInfo> findDestinations(T data) throws HashException, PartitionException;

}
