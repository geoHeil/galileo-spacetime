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

package galileo.test.dht.partitioning;

import static org.junit.Assert.*;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureSet;
import galileo.dht.GroupInfo;
import galileo.dht.NetworkInfo;
import galileo.dht.NodeInfo;
import galileo.dht.PartitionException;
import galileo.dht.SpatialHierarchyPartitioner;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashTopologyException;

import org.junit.Test;

/**
 * Tests the {@link SpatialHierarchyPartitioner}.  This test uses a hardcoded
 * set of locations to verify that partitioning functionality has not regressed.
 */
public class SpatialHierarchy {

    private NetworkInfo ni;
    private SpatialHierarchyPartitioner partitioner;

    private String[] geohashes = { "c2", "c8", "cb", "f0", "f2",
        "9r", "9x", "9z", "dp", "dr",
        "9q", "9w", "9y", "dn", "dq",
        "9m", "9t", "9v", "dj" };

    public SpatialHierarchy()
    throws HashException, HashTopologyException, PartitionException {
        ni = new NetworkInfo();
        ni.addGroup(new GroupInfo("test group"));
        ni.addGroup(new GroupInfo("group2"));

        GroupInfo group = ni.getGroups().get(0);
        group.addNode(new NodeInfo("lattice-1", 5555));
        group.addNode(new NodeInfo("lattice-2", 5555));
        group.addNode(new NodeInfo("lattice-3", 5555));
        group.addNode(new NodeInfo("lattice-4", 5555));
        group.addNode(new NodeInfo("lattice-5", 5555));
        group.addNode(new NodeInfo("lattice-6", 5555));
        group.addNode(new NodeInfo("lattice-7", 5555));

        GroupInfo group2 = ni.getGroups().get(1);
        group2.addNode(new NodeInfo("lattice-8", 5555));
        group2.addNode(new NodeInfo("lattice-9", 5555));
        group2.addNode(new NodeInfo("lattice-10", 5555));
        group2.addNode(new NodeInfo("lattice-11", 5555));
        group2.addNode(new NodeInfo("lattice-12", 5555));

        partitioner = new SpatialHierarchyPartitioner(null, ni, geohashes);
    }

    @Test
    public void testGroup1() throws Exception {
        TemporalProperties tp = new TemporalProperties(System.nanoTime());
        SpatialProperties sp = new SpatialProperties(39.16f, -106.2f);

        FeatureSet fs = new FeatureSet();
        fs.put(new Feature("test1", 36.2));

        Metadata meta = new Metadata();
        meta.setTemporalProperties(tp);
        meta.setSpatialProperties(sp);
        meta.setAttributes(fs);

        assertEquals("block1", "lattice-1:5555",
                partitioner.locateData(meta).toString());

        fs.put(new Feature("snow_depth", 18.2));
        assertEquals("block2", "lattice-1:5555",
                partitioner.locateData(meta).toString());

        fs.put(new Feature("temperature", 32.3));
        fs.put(new Feature("humidity", 33.3));
        assertEquals("block3", "lattice-4:5555",
                partitioner.locateData(meta).toString());

        fs.put(new Feature("wind_velocity", 55.2));
        fs.put(new Feature("featureificness", 100));
        assertEquals("block4", "lattice-3:5555",
                partitioner.locateData(meta).toString());

        sp = new SpatialProperties(29.55f, -111.19f);
        meta.setSpatialProperties(sp);
        assertEquals("block5", "lattice-3:5555",
                partitioner.locateData(meta).toString());
    }

    @Test
    public void testGroup2() throws Exception {
        TemporalProperties tp = new TemporalProperties(System.nanoTime());
        SpatialProperties sp = new SpatialProperties(42.36f, -85.37f);

        FeatureSet fs = new FeatureSet();
        fs.put(new Feature("test1", 36.2));

        Metadata meta = new Metadata();
        meta.setTemporalProperties(tp);
        meta.setSpatialProperties(sp);
        meta.setAttributes(fs);

        /* Note how the relative positions should be the same in this group! */
        assertEquals("block1", "lattice-8:5555",
                partitioner.locateData(meta).toString());

        fs.put(new Feature("temperature", 32.3));
        fs.put(new Feature("humidity", 33.3));
        assertEquals("block2", "lattice-8:5555",
                partitioner.locateData(meta).toString());

        fs.put(new Feature("wind_velocity", 55.2));
        fs.put(new Feature("featureificness", 100));
        assertEquals("block3", "lattice-11:5555",
                partitioner.locateData(meta).toString());
    }

    @Test(expected = galileo.dht.hash.HashException.class)
    public void testOutofBounds() throws Exception {
        TemporalProperties tp = new TemporalProperties(System.nanoTime());
        /* Around the north pole... */
        SpatialProperties sp = new SpatialProperties(87.90f, -154.06f);

        FeatureSet fs = new FeatureSet();
        fs.put(new Feature("test1", 36.2));

        Metadata meta = new Metadata();
        meta.setTemporalProperties(tp);
        meta.setSpatialProperties(sp);
        meta.setAttributes(fs);

        /* Note how the relative positions should be the same in this group! */
        assertEquals("block1", "lattice-8:5555",
                partitioner.locateData(meta).toString());
    }
}
