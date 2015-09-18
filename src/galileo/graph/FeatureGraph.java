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

package galileo.graph;

import galileo.dataset.feature.Feature;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;

import java.io.IOException;

/**
 * A reduced-resolution version of the MetadataGraph.
 *
 * @author malensek
 */
public class FeatureGraph extends MetadataGraph {

    public FeatureGraph() {
        super();
    }

    public FeatureGraph(FeatureHierarchy hierarchy) {
        super(hierarchy);
    }

    @Override
    public void addPath(Path<Feature, String> path)
    throws FeatureTypeMismatchException, GraphException {
        Path<Feature, String> qPath = quantizePath(path);
        graph.addPath(qPath);
    }

    private Path<Feature, String> quantizePath(Path<Feature, String> path) {
        Path<Feature, String> newPath = new Path<Feature, String>();
        for (Vertex<Feature, String> v : path.getVertices()) {
            Feature oldFeature = v.getLabel();
//            Feature newFeature = new Feature(oldFeature.getName(),
//                    Math.round(oldFeature.getFloat()));
            Feature newFeature = new Feature(oldFeature.getName(),
                    oldFeature.getInt());
            Vertex<Feature, String> newVertex = new Vertex<>(newFeature);
            newPath.add(newVertex);
        }
        newPath.setPayload(path.getPayload());
        return newPath;
    }

    @Deserialize
    public FeatureGraph(SerializationInputStream in)
    throws GraphException, IOException, SerializationException {
        super(in);
    }
}
