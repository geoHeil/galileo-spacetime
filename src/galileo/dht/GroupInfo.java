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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Records network group information.  Groups can contain arbitrary subgroups,
 * and, of course, a number of nodes.
 *
 * @author malensek, jkachika
 */
public class GroupInfo {

    private String name;

    List<NodeInfo> nodes;
    List<GroupInfo> groups;
    Set<String> geohashes;

    public GroupInfo(String name) {
        this.name = name;
        this.nodes = new ArrayList<>();
        this.groups = new ArrayList<>();
        this.geohashes = new HashSet<String>();
    }
    
    public void addGeoHash(String geoHash){
    	this.geohashes.add(geoHash);
    }
    
    public Set<String> getGeoHashes(){
    	return new HashSet<String>(this.geohashes);
    }
    
    public boolean hasGeoHash(String geohash){
    	return this.geohashes.contains(geohash);
    }

    public void addNode(NodeInfo node) {
        nodes.add(node);
    }

    public List<NodeInfo> getNodes() {
        return nodes;
    }

    public void addGroup(GroupInfo group) {
        groups.add(group);
    }

    public List<GroupInfo> getGroups() {
        return groups;
    }

    public List<NodeInfo> getAllNodes() {
        List<NodeInfo> nodeList = new ArrayList<>();
        for (GroupInfo group : groups) {
            nodeList.addAll(group.getAllNodes());
        }
        nodeList.addAll(nodes);
        return nodeList;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        String str = "Group: " + name + System.lineSeparator();
        for (GroupInfo group : groups) {
            str += "    " + group + System.lineSeparator();
        }
        for (NodeInfo node : nodes) {
            str += "    " + node + System.lineSeparator();
        }
        return str;
    }
}
