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

import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureArray;
import galileo.dataset.feature.FeatureArraySet;
import galileo.dataset.feature.FeatureType;
import galileo.serialization.Serializer;

import java.util.Random;

import org.junit.Test;

/**
 * Tests FeatureArraySet functionality and serialization.
 */
public class ArraySet {

    private FeatureArray generate3DArray(String name, int h, int w, int d) {
        Random rand = new Random();
        FeatureArray f = new FeatureArray(name, FeatureType.LONG, h, w, d);
        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    f.set(new Feature(rand.nextLong()), i, j, k);
                }
            }
        }
        return f;
    }

    private FeatureArray generate3DArray(int h, int w, int d) {
        Random rand = new Random();
        FeatureArray f = new FeatureArray(h, w, d);
        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    f.set(new Feature(rand.nextLong()), i, j, k);
                }
            }
        }
        return f;
    }

    @Test
    public void testSerialization()
    throws Exception {

        FeatureArraySet fset = new FeatureArraySet();
        fset.put(generate3DArray("arr1", 100, 300, 10));
        fset.put(generate3DArray("arr2", 10, 200, 2));
        fset.put(generate3DArray("arr3", 10, 30, 100));
        fset.put(generate3DArray("arr4", 30, 10, 10));
        fset.put(generate3DArray(2, 2, 1000));

        String[] s = { "test1", "test2", "test3" };
        FeatureArray strArray = new FeatureArray("strings", s);
        fset.put(strArray);

        byte[] setb = Serializer.serialize(fset);
        //System.out.println("Byte[] size: " + setb.length);

        FeatureArraySet fset2
            = Serializer.deserialize(FeatureArraySet.class, setb);

        for (FeatureArray fa : fset2) {
            String name = fa.getName();
            FeatureArray f1 = fset.get(name);
            FeatureArray f2 = fset2.get(name);

            assertEquals("named", f1.isNamed(), f2.isNamed());
            assertEquals("name", f1.getName(), f2.getName());
            assertEquals("typed", f1.isTyped(), f2.isTyped());
            assertEquals("type", f1.getType(), f2.getType());
            assertEquals("size", f1.getSize(), f2.getSize());
            assertEquals("rank", f1.getRank(), f2.getRank());
            assertEquals("rank", f1.getDimensions()[0], f2.getDimensions()[0]);

            if (f1.getRank() == 3) {
                assertEquals("element", f1.get(0, 0, 0), f2.get(0, 0, 0));
            } else if (f1.getRank() == 1) {
                assertEquals("element", f1.get(0), f2.get(0));
            }
        }
    }
}
