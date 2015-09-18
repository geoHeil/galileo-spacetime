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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Provides a lightweight generic implementation of a graph vertex backed by a
 * TreeMap for extensibility.  This provides the basis of the hybrid
 * trees/graphs used in the system.
 *
 * @author malensek
 */
public class Vertex<L extends Comparable<L>, V> {

    protected L label;
    protected Set<V> values = new HashSet<V>();
    protected TreeMap<L, Vertex<L, V>> edges = new TreeMap<>();

    public Vertex() { }

    public Vertex(L label) {
        this.label = label;
    }

    public Vertex(L label, V value) {
        this.label = label;
        this.addValue(value);
    }

    public Vertex(L label, Collection<V> values) {
        this.label = label;
        this.addValues(values);
    }

    public Vertex(Vertex<L, V> v) {
        this.label = v.label;
    }

    /**
     * Determines if two vertices are connected.
     *
     * @return true if the Vertex label is found on a connecting edge.
     */
    public boolean connectedTo(L label) {
        return edges.containsKey(label);
    }

    /**
     * Retrieve a neighboring Vertex.
     *
     * @param label Neighbor's label.
     *
     * @return Neighbor Vertex.
     */
    public Vertex<L, V> getNeighbor(L label) {
        return edges.get(label);
    }

    public NavigableMap<L, Vertex<L, V>> getNeighborsLessThan(
            L label, boolean inclusive) {
        return edges.headMap(label, inclusive);
    }

    public NavigableMap<L, Vertex<L, V>> getNeighborsGreaterThan(
            L label, boolean inclusive) {
        return edges.tailMap(label, inclusive);
    }

    /**
     * Retrieve the labels of all neighboring vertices.
     *
     * @return Neighbor Vertex labels.
     */
    public Set<L> getNeighborLabels() {
        return edges.keySet();
    }

    /**
     * Traverse all edges to return all neighboring vertices.
     *
     * @return collection of all neighboring vertices.
     */
    public Collection<Vertex<L, V>> getAllNeighbors() {
        return edges.values();
    }

    /**
     * Connnects two vertices.  If this vertex is already connected to the
     * provided vertex label, then the already-connected vertex is returned, and
     * its value is updated.
     *
     * @param vertex The vertex to connect to.
     *
     * @return Connected vertex.
     */
    public Vertex<L, V> connect(Vertex<L, V> vertex) {
        L label = vertex.getLabel();
        Vertex<L, V> edge = getNeighbor(label);
        if (edge == null) {
            edges.put(label, vertex);
            return vertex;
        } else {
            edge.addValues(vertex.getValues());
            return edge;
        }
    }

    /**
     * Add and connect a collection of vertices in the form of a traversal path.
     */
    public void addPath(Iterator<Vertex<L, V>> path) {
        if (path.hasNext()) {
            Vertex<L, V> vertex = path.next();
            Vertex<L, V> edge = connect(vertex);
            edge.addPath(path);
        }
    }

    public L getLabel() {
        return label;
    }

    public void setLabel(L label) {
        this.label = label;
    }

    public Set<V> getValues() {
        return values;
    }

    public void addValue(V value) {
        this.values.add(value);
    }

    public void addValues(Collection<V> values) {
        this.values.addAll(values);
    }

    /**
     * Retrieves all {@link Path} instances represented by the children of this
     * Vertex.
     *
     * @return List of Paths that are descendants of this Vertex
     */
    public List<Path<L, V>> descendantPaths() {
        Path<L, V> p = new Path<L, V>();
        List<Path<L, V>> paths = new ArrayList<>();
        for (Vertex<L, V> child : this.getAllNeighbors()) {
            traverseDescendants(child, paths, p);
        }

        return paths;
    }

    /**
     * Traverses through descendant Vertices, finding Path instances.  A Path
     * leads to one or more payloads stored as Vertex values.  This method is
     * designed to be used recursively.
     *
     * @param vertex Vertex to query descendants
     * @param paths List of Paths discovered thus far during traversal.  This is
     * updated as new Path instances are found.
     * @param currentPath The current Path being inspected by the traversal
     */
    protected void traverseDescendants(Vertex<L, V> vertex,
            List<Path<L, V>> paths, Path<L, V> currentPath) {

        Path<L, V> p = new Path<>(currentPath);
        p.add(new Vertex<>(vertex));

        if (vertex.getValues().size() > 0) {
            /* If the vertex has values, we've found a path endpoint. */
            p.setPayload(vertex.getValues());
            paths.add(p);
        }

        for (Vertex<L, V> child : vertex.getAllNeighbors()) {
            traverseDescendants(child, paths, p);
        }
    }

    /**
     * Retrieves the number of descendant vertices for this {@link Vertex}.
     *
     * @return number of descendants (children)
     */
    public long numDescendants() {
        long total = this.getAllNeighbors().size();
        for (Vertex<L, V> child : this.getAllNeighbors()) {
            total += child.numDescendants();
        }

        return total;
    }

    /**
     * Retrieves the number of descendant edges for this {@link Vertex}.  This
     * count includes the links between descendants for scan operations.
     *
     * @return number of descendant edges.
     */
    public long numDescendantEdges() {
        long total = 0;
        int numNeighbors = this.getAllNeighbors().size();

        if (numNeighbors > 0) {
            total = numNeighbors + numNeighbors - 1;
        }

        for (Vertex<L, V> child : this.getAllNeighbors()) {
            total += child.numDescendantEdges();
        }

        return total;
    }

    /**
     * Removes all the edges from this Vertex, severing any connections with
     * neighboring vertices.
     */
    public void clearEdges() {
        edges.clear();
    }

    /**
     * Clears all values associated with this Vertex.
     */
    public void clearValues() {
        values.clear();
    }

    /**
     * Pretty-print this vertex (and its children) with a given indent level.
     */
    protected String toString(int indent) {
        String ls = System.lineSeparator();
        String str = "(" + getLabel() + " " + values + ")" + ls;

        String space = " ";
        for (int i = 0; i < indent; ++i) {
            space += "|  ";
        }
        space += "|-";
        ++indent;

        for (Vertex<L, V> vertex : edges.values()) {
            str += space + vertex.toString(indent);
        }

        return str;
    }

    @Override
    public String toString() {
        return toString(0);
    }
}
