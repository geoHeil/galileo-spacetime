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

import java.util.List;
import java.util.Set;

import galileo.dataset.feature.Feature;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Query;

/**
 * Contains a graph {@link Path} composed of Features.
 *
 * @author malensek
 */
public class FeaturePath<V> extends Path<Feature, V> {

    public FeaturePath(Feature... features) {
        for (Feature f : features) {
            this.add(new Vertex<Feature, V>(f));
        }
    }

    public FeaturePath(V payload, Feature... features) {
        this(features);
        this.addPayload(payload);
    }

    public FeaturePath(Set<V> payload, Feature... features) {
        this(features);
        this.setPayload(payload);
    }

    /**
     * Evaluates this path against a provided {@link Query} instance.  If any of
     * the {@link Operation}s in the Query are satisfied by this Path instance,
     * this method will return true.
     *
     * @param query Query to evaluate against this Path instance
     *
     * @return true if the Query is satisfied by this path, or false otherwise.
     */
    public boolean satisfiesQuery(Query query)
    throws GraphException {
        for (Operation operation : query.getOperations()) {
            if (this.satisfiesOperation(operation)) {
                return true;
            }
        }

        /* None of the query operations were satisfied by this path */
        return false;
    }

    private boolean satisfiesOperation(Operation operation)
    throws GraphException {
        for (Vertex<Feature, V> vertex : this.getVertices()) {
            Feature feature = vertex.getLabel();
            List<Expression> expressions
                = operation.getOperand(feature.getName());

            if (expressions == null) {
                continue;
            }

            for (Expression expression : expressions) {
                if (this.satisfiesExpression(feature, expression) == false) {
                    /* All expressions within an operation must be satisfied. */
                    return false;
                }
            }
        }

        /* All Expressions in the Operation were satisfied by this path */
        return true;
    }

    private boolean satisfiesExpression(
            Feature feature, Expression expression)
    throws GraphException {

        Feature value = expression.getValue();

        switch (expression.getOperator()) {
            case EQUAL:
                return (feature.equals(value) == true);

            case NOTEQUAL:
                return (feature.equals(value) == false);

            case LESS:
                return feature.less(value);

            case LESSEQUAL:
                return (feature.less(value) || feature.equals(value));

            case GREATER:
                return feature.greater(value);

            case GREATEREQUAL:
                return (feature.greater(value) || feature.equals(value));

            case UNKNOWN:
            default:
                throw new GraphException("Invalid operator");
        }
    }
}
