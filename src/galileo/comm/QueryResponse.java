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

package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import galileo.dataset.feature.Feature;
import galileo.event.Event;
import galileo.graph.FeaturePath;
import galileo.graph.GraphException;
import galileo.graph.Path;
import galileo.graph.Vertex;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class QueryResponse implements Event {

    private String id;
    private List<Path<Feature, String>> results;

    public QueryResponse(String id, List<Path<Feature, String>> results) {
        this.id = id;
        this.results = results;
    }

    public String getId() {
        return id;
    }

    public List<Path<Feature, String>> getResults() {
        return results;
    }

    public QueryResponse(SerializationInputStream in)
    throws IOException, SerializationException, GraphException {
        id = in.readString();
        int numResults = in.readInt();
        results = new ArrayList<>(numResults);
        for (int i = 0; i < numResults; ++i) {
            FeaturePath<String> p = new FeaturePath<>();
            int numVertices = in.readInt();
            for (int vertex = 0; vertex < numVertices; ++vertex) {
                Feature f = new Feature(in);
                Vertex<Feature, String> v = new Vertex<>(f);
                p.add(v);
            }

            int numPayloads = in.readInt();
            for (int payload = 0; payload < numPayloads; ++payload) {
                String pay = in.readString();
                p.addPayload(pay);
            }

            results.add(p);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeString(id);
        out.writeInt(results.size());
        for (Path<Feature, String> path : results) {
            List<Vertex<Feature, String>> vertices = path.getVertices();
            out.writeInt(vertices.size());
            for (Vertex<Feature, String> v : vertices) {
                out.writeSerializable(v.getLabel());
            }

            Collection<String> payload = path.getPayload();
            out.writeInt(payload.size());
            for (String item : payload) {
                out.writeString(item);
            }
        }
    }
}
