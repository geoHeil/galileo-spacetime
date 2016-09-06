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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.json.JSONArray;

import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureType;
import galileo.query.PayloadFilter;
import galileo.query.Query;
import galileo.serialization.ByteSerializable;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.util.Pair;

public class MetadataGraph implements ByteSerializable {

    HierarchicalGraph<String> graph;

    public MetadataGraph() {
        graph = new HierarchicalGraph<>();
    }

    public MetadataGraph(FeatureHierarchy hierarchy) {
        graph = new HierarchicalGraph<>(hierarchy);
    }

    public void addPath(Path<Feature, String> path)
    throws FeatureTypeMismatchException, GraphException {
        graph.addPath(path);
    }

    /**
     * Reorients this graph to match a given {@link FeatureHierarchy}.  If the
     * FeatureHierarchy does not define positions for all of the Features
     * present in this graph, they will be assigned on a first-come,
     * first-served basis.
     *
     * @param hierarchy the new FeatureHierarchy this graph should take on.
     */
    public void reorient(FeatureHierarchy hierarchy)
    throws FeatureTypeMismatchException, GraphException {
        List<Path<Feature, String>> paths = graph.getAllPaths();
        graph = new HierarchicalGraph<>(hierarchy);
        for(Path<Feature, String> path : paths) {
            addPath(path);
        }
    }

    public List<Path<Feature, String>> evaluateQuery(Query query) {
        return graph.evaluateQuery(query);
    }
    
    
    public JSONArray getFeaturesJSON(){
    	return graph.getFeaturesJSON();
    }
    
    public FeatureHierarchy getFeatureHierarchy(){
    	return graph.getFeatureHierarchy();
    }

    public List<Path<Feature, String>> evaluateQuery(Query query,
            PayloadFilter<String> filter) {
        return graph.evaluateQuery(query, filter);
    }

    public static MetadataGraph fromPaths(List<Path<Feature, String>> paths) {
        MetadataGraph m = new MetadataGraph();
        for (Path<Feature, String> path : paths) {
            try {
                m.addPath(path);
            } catch (Exception e) {
                //TODO log this? throw?
                e.printStackTrace();
            }
        }
        return m;
    }

    public List<Path<Feature, String>> getAllPaths() {
        return graph.getAllPaths();
    }

    public long numVertices() {
        return graph.getRoot().numDescendants();
    }

    public long numEdges() {
        return graph.getRoot().numDescendantEdges();
    }

    @Override
    public String toString() {
        return graph.toString();
    }

    @Deserialize
    public MetadataGraph(SerializationInputStream in)
    throws GraphException, IOException, SerializationException {
        FeatureHierarchy hierarchy = new FeatureHierarchy();
        int numLevels = in.readInt();
        for (int level = 0; level < numLevels; ++level) {
            String name = in.readString();
            FeatureType type = FeatureType.fromInt(in.readInt());
            hierarchy.addFeature(name, type);
        }

        graph = new HierarchicalGraph<String>(hierarchy);

        int numPaths = in.readInt();
        for (int path = 0; path < numPaths; ++path) {
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

            try {
                this.addPath(p);
            } catch (FeatureTypeMismatchException e) {
                throw new SerializationException("Could not add deserialized "
                        + "path to the MetadataGraph.", e);
            }
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        FeatureHierarchy hierarchy = graph.getFeatureHierarchy();
        out.writeInt(hierarchy.size());
        for (Pair<String, FeatureType> level : hierarchy) {
            out.writeString(level.a);
            out.writeInt(level.b.toInt());
        }

        List<Path<Feature, String>> paths = graph.getAllPaths();
        out.writeInt(paths.size());
        for (Path<Feature, String> path : paths) {
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
