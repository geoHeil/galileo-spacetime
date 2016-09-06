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

package galileo.test.graph;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.dataset.feature.Feature;
import galileo.fs.GeospatialFileSystem;
import galileo.graph.MetadataGraph;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Query;
import galileo.samples.ConvertNetCDF;

public class QueryTest {
    public static void main(String[] args) throws Exception {
        String fileName = args[0];
        List<Block> blocks = new ArrayList<>();
        if (!fileName.equals("cached")) {
            /* If the file name is just "cached" then try to load the filesystem
             * from its previous state (if files have been already stored in the
             * FS).  Otherwise, we'll load the file specified and insert it into
             * the FS.
             */
            Map<String, Metadata> metas = ConvertNetCDF.readFile(fileName);
            for (Map.Entry<String, Metadata> entry : metas.entrySet()) {
                blocks.add(ConvertNetCDF.createBlock("", entry.getValue()));
            }
        }

        GeospatialFileSystem gfs = new GeospatialFileSystem("/var/tmp/galtest", "test");

        /* Insert the blocks we've loaded, if any */
        if (blocks.size() > 0) {
            for (Block block : blocks) {
                gfs.storeBlock(block);
            }
        }

        /* Execute some queries */
        Query q = new Query();
        /* Each expression chained together in an operator produces a logical
         * AND */
        q.addOperation(new Operation(
                    new Expression("==", new Feature("total_precipitation", 0.0f)),
                    new Expression("==", new Feature("precipitable_water", 4.2375383f)),
                    new Expression("==", new Feature("temperature_surface", 257.41336f))));
        System.out.println("Query: " + q);
        MetadataGraph result = MetadataGraph.fromPaths(gfs.query(q));
        System.out.println(result);

        gfs.shutdown();
    }
}
