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

package galileo.test.graph;

import static org.junit.Assert.*;
import org.junit.Test;

import galileo.dataset.feature.Feature;
import galileo.graph.FeaturePath;

import galileo.graph.GraphException;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Query;

public class FeaturePathQuery {

    @Test
    public void singleOperationPositiveQueries()
    throws GraphException {
        FeaturePath<String> fp = new FeaturePath<>("test path",
                new Feature("humidity", 32.3),
                new Feature("wind_speed", 5.0),
                new Feature("temperature", 274.8));

        Query q = new Query();
        q.addOperation(new Operation(
                    new Expression("<", new Feature("humidity", 64.8))));

        assertEquals("humidity less than", true, fp.satisfiesQuery(q));

        q = new Query();
        q.addOperation(new Operation(
                    new Expression("==", new Feature("wind_speed", 5.0))));

        assertEquals("wind equals", true, fp.satisfiesQuery(q));

        q = new Query();
        q.addOperation(new Operation(
                    new Expression("!=", new Feature("temperature", 1.0))));

        assertEquals("temperature not", true, fp.satisfiesQuery(q));
    }

    @Test
    public void singleOperationNegativeQueries()
    throws GraphException {
        FeaturePath<String> fp = new FeaturePath<>("test path",
                new Feature("humidity", 32.3),
                new Feature("wind_speed", 5.0),
                new Feature("temperature", 274.8));

        Query q = new Query();
        q.addOperation(new Operation(
                    new Expression(">", new Feature("humidity", 64.8))));

        assertEquals("humidity greater than", false, fp.satisfiesQuery(q));

        q = new Query();
        q.addOperation(new Operation(
                    new Expression("==", new Feature("wind_speed", 6.0))));

        assertEquals("wind equals", false, fp.satisfiesQuery(q));

        q = new Query();
        q.addOperation(new Operation(
                    new Expression("!=", new Feature("temperature", 274.8))));

        assertEquals("temperature not", false, fp.satisfiesQuery(q));
    }

    @Test
    public void multiExpressionQueries()
    throws GraphException {
        FeaturePath<String> fp = new FeaturePath<>("test path",
                new Feature("humidity", 32.3),
                new Feature("wind_speed", 5.0),
                new Feature("temperature", 274.8));

        Query q = new Query();
        q.addOperation(new Operation(
                    new Expression(">", new Feature("humidity", 64.8)),
                    new Expression("<", new Feature("temperature", 300.0))));

        assertEquals("humidity and temp", false, fp.satisfiesQuery(q));

        q = new Query();
        q.addOperation(new Operation(
                    new Expression("==", new Feature("wind_speed", 6.0)),
                    new Expression(">", new Feature("wind_speed", 0.5))));

        assertEquals("wind equals and greater", false, fp.satisfiesQuery(q));

        q = new Query();
        q.addOperation(new Operation(
                    new Expression("!=", new Feature("temperature", 274.8)),
                    new Expression("==", new Feature("wind_speed", 8.0))));

        assertEquals("temperature not", false, fp.satisfiesQuery(q));

        FeaturePath<String> fp2 = new FeaturePath<>("test path 2",
                new Feature("f1", 33.2),
                new Feature("f2", 16.6),
                new Feature("f3", 11.1),
                new Feature("f4", 12.0));

        /* Make sure that one 'true' value doesn't cause the whole query to be
         * considered satisfied */
        q = new Query();
        q.addOperation(new Operation(
                    new Expression("<", new Feature("f1", 40.0)),
                    new Expression(">", new Feature("f2", 20.0))));
        assertEquals("less and greater", false, fp2.satisfiesQuery(q));

        /* Other way */
        q = new Query();
        q.addOperation(new Operation(
                    new Expression(">", new Feature("f2", 20.0)),
                    new Expression("<", new Feature("f1", 40.0))));
        assertEquals("greater and less", false, fp2.satisfiesQuery(q));
    }

    @Test
    public void multiOperationQueries()
    throws GraphException {
        FeaturePath<String> fp = new FeaturePath<>("test path",
                new Feature("humidity", 32.3),
                new Feature("wind_speed", 5.0),
                new Feature("temperature", 274.8),
                new Feature("snow", 3.8));

        Query q;

        q = new Query();

        /* Add an operation that won't work. */
        q.addOperation(new Operation(
                    new Expression("<", new Feature("humidity", 10.0))));

        assertEquals("Bad op", false, fp.satisfiesQuery(q));

        /* Now add another operation that should satisfy the query */
        q.addOperation(new Operation(
                    new Expression("==", new Feature("snow", 3.8))));
        assertEquals("Bad op Good op", true, fp.satisfiesQuery(q));
    }

    @Test
    public void testAllEqual()
    throws GraphException {
        FeaturePath<String> fp = new FeaturePath<>("test path",
                new Feature("humidity", 32.3),
                new Feature("wind_speed", 5.0),
                new Feature("temperature", 274.8),
                new Feature("snow", 3.8));

        Query q = new Query();
        q.addOperation(new Operation(
                    new Expression("==", new Feature("humidity", 32.3)),
                    new Expression("==", new Feature("wind_speed", 5.0)),
                    new Expression("==", new Feature("temperature", 274.8)),
                    new Expression("==", new Feature("snow", 3.8))));
        assertEquals("all equal", true, fp.satisfiesQuery(q));
    }
}
