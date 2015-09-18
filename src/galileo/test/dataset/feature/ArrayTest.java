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
import galileo.dataset.feature.FeatureType;
import galileo.serialization.Serializer;

import java.util.Random;

import org.junit.Test;

/**
 * Tests FeatureArray functionality.  Includes serialization and native array
 * conversion.
 */
public class ArrayTest {

    /**
     * Tests serialization back and forth with a native int array as a
     * source.
     */
    @Test
    public void testJavaIntArray()
    throws Exception {

        final int h = 100;
        final int w = 300;
        final int d = 10;

        Random rand = new Random();
        Integer[][][] arr = new Integer[h][w][d];
        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    arr[i][j][k] = rand.nextInt();
                }
            }
        }

        FeatureArray f = new FeatureArray("parameter1", arr);
        int rank = f.getRank();
        int size = f.getSize();
        FeatureType type = f.getType();
        String name = f.getName();

        byte[] fabytes = Serializer.serialize(f);
        FeatureArray f2 = Serializer.deserialize(FeatureArray.class, fabytes);

        assertEquals("Rank", rank, f2.getRank());
        assertEquals("Size", size, f2.getSize());
        assertEquals("Type", type, f2.getType());
        assertEquals("Name", name, f2.getName());

        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    assertEquals("Value", f.get(i, j, k), f2.get(i, j, k));
                }
            }
        }
    }

    /**
     * Tests serialization with floats
     */
    @Test
    public void testJavaFloatArray()
    throws Exception {

        final int h = 10;
        final int w = 300;
        final int d = 100;

        Random rand = new Random();
        Float[][][] arr = new Float[h][w][d];
        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    arr[i][j][k] = rand.nextFloat();
                }
            }
        }

        FeatureArray f = new FeatureArray("floating", arr);
        int rank = f.getRank();
        int size = f.getSize();
        FeatureType type = f.getType();
        String name = f.getName();

        byte[] fabytes = Serializer.serialize(f);
        FeatureArray f2 = Serializer.deserialize(FeatureArray.class, fabytes);

        assertEquals("Rank", rank, f2.getRank());
        assertEquals("Size", size, f2.getSize());
        assertEquals("Type", type, f2.getType());
        assertEquals("Name", name, f2.getName());

        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    /* Convert to integers */
                    assertEquals("Value", f.get(i, j, k).getInt(),
                            f2.get(i, j, k).getInt());

                    /* Check actual values */
                    assertEquals("Value", f.get(i, j, k), f2.get(i, j, k));
                }
            }
        }
    }

    /**
     * Uses a Galileo native FeatureArray instead of Java native arrays.
     * This test also does not include a type or name for the data.
     */
    @Test
    public void testNativeFeatureArray()
    throws Exception {

        final int h = 50;
        final int w = 50;
        final int d = 100;

        Random rand = new Random();
        FeatureArray f = new FeatureArray(h, w, d);
        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    f.set(new Feature(rand.nextLong()), i, j, k);
                }
            }
        }

        int rank = f.getRank();
        int size = f.getSize();
        FeatureType type = f.getType();
        String name = f.getName();

        byte[] fabytes = Serializer.serialize(f);
        FeatureArray f2 = Serializer.deserialize(FeatureArray.class, fabytes);

        assertEquals("Rank", rank, f2.getRank());
        assertEquals("Size", size, f2.getSize());
        assertEquals("Type", type, f2.getType());
        assertEquals("Name", name, f2.getName());

        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    assertEquals("Value", f.get(i, j, k), f2.get(i, j, k));
                }
            }
        }
    }

    /**
     * Tests a big 1D array
     */
    @Test
    public void test1DSerialization()
    throws Exception {

        final int h = 50000;

        Random rand = new Random();
        FeatureArray f = new FeatureArray(h);
        for (int i = 0; i < h; ++i) {
            f.set(new Feature(rand.nextDouble()), i);
        }

        int rank = f.getRank();
        int size = f.getSize();
        FeatureType type = f.getType();
        String name = f.getName();

        byte[] fabytes = Serializer.serialize(f);
        FeatureArray f2 = Serializer.deserialize(FeatureArray.class, fabytes);

        assertEquals("Rank", rank, f2.getRank());
        assertEquals("Size", size, f2.getSize());
        assertEquals("Type", type, f2.getType());
        assertEquals("Name", name, f2.getName());

        for (int i = 0; i < h; ++i) {
            assertEquals("Value", f.get(i), f2.get(i));
        }
    }

    /**
     * Tests a 6D array.
     */
    @Test
    public void testManyDimensionArray()
    throws Exception {

        final int h = 5;
        final int w = 10;
        final int d = 5;
        final int x = 3;
        final int y = 8;
        final int z = 2;

        Random rand = new Random();
        FeatureArray f = new FeatureArray(h, w, d, x, y, z);
        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    for (int l = 0; l < x; ++l) {
                        for (int m = 0; m < y; ++m) {
                            for (int n = 0; n < z; ++n) {
                                f.set(new Feature(rand.nextLong()),
                                        i, j, k, l, m, n);
                            }
                        }
                    }
                }
            }
        }

        int rank = f.getRank();
        int size = f.getSize();
        FeatureType type = f.getType();
        String name = f.getName();

        byte[] fabytes = Serializer.serialize(f);
        FeatureArray f2 = Serializer.deserialize(FeatureArray.class, fabytes);

        assertEquals("Rank", rank, f2.getRank());
        assertEquals("Size", size, f2.getSize());
        assertEquals("Type", type, f2.getType());
        assertEquals("Name", name, f2.getName());

        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                for (int k = 0; k < d; ++k) {
                    for (int l = 0; l < x; ++l) {
                        for (int m = 0; m < y; ++m) {
                            for (int n = 0; n < z; ++n) {
                                assertEquals("Value", f.get(i, j, k, l, m, n),
                                        f2.get(i, j, k, l, m, n));
                            }
                        }
                    }
                }
            }
        }
    }

}
