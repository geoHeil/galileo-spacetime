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

package galileo.test.dataset.feature;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import galileo.dataset.feature.Feature;
import galileo.serialization.SerializationException;
import galileo.serialization.Serializer;

public class Serialization {

    @Test
    public void testInteger() throws Exception {
        testSerialization(new Feature("test", 32));
    }

    @Test
    public void testLong() throws Exception {
        testSerialization(new Feature("test", 32L));
    }

    @Test
    public void testFloat() throws Exception {
        testSerialization(new Feature("test", 1.25f));
    }

    @Test
    public void testDouble() throws Exception {
        testSerialization(new Feature("test", 3.6d));
    }

    @Test
    public void testNull() throws Exception {
        testSerialization(new Feature("test"));
    }

    @Test
    public void testString() throws Exception {
        testSerialization(new Feature("test", "testing string!"));
    }

    @Test
    public void testByte() throws Exception {
        testSerialization(new Feature("test", new byte[] { 1, 2, 3 }));
    }

    @Test
    public void testIntegerInterval() throws Exception {
        testSerialization(new Feature("interval", 11, 27));
    }

    @Test
    public void testLongInterval() throws Exception {
        testSerialization(new Feature("interval", 69L, 23L));
    }

    @Test
    public void testFloatInterval() throws Exception {
        testSerialization(new Feature("interval", 1337.33f, 1582.99f));
    }

    @Test
    public void testDoubleInterval() throws Exception {
        testSerialization(new Feature("interval", 1337.00d, 1000345.234d));
    }

    private void testSerialization(Feature f1)
    throws IOException, SerializationException {
        byte[] bytes = Serializer.serialize(f1);
        Feature f2 = Serializer.deserialize(Feature.class, bytes);
        assertEquals("Equality", f1, f2);
    }
}
