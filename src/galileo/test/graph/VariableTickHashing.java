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

import static org.junit.Assert.*;

import galileo.dataset.feature.Feature;
import galileo.graph.TickHash;

import org.junit.Test;

public class VariableTickHashing {

    @Test
    public void testZeroToFiftyRange() {
        TickHash t = new TickHash();
        t.addTick(new Feature("temperature", 0.0));
        t.addTick(new Feature("temperature", 10.0));
        t.addTick(new Feature("temperature", 20.0));
        t.addTick(new Feature("temperature", 30.0));
        t.addTick(new Feature("temperature", 50.0));
        System.out.println("TickHash contents:");
        System.out.println(t);

        Feature f1 = t.getBucket(new Feature("temperature", 33.0));
        Feature f2 = t.getBucket(new Feature("temperature", 99.0));
        Feature f3 = t.getBucket(new Feature("temperature", -42.0));

        assertEquals("30 group", new Feature("temperature", 30.0), f1);
        assertEquals("50 group", new Feature("temperature", 50.0), f2);
        assertEquals("0 group", new Feature("temperature", 0.0), f3);

    }


}
