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

package galileo.test.stat;

import static org.junit.Assert.*;
import org.junit.Test;

import galileo.stat.RunningStatistics;

/**
 * Tests the RunningStatistics implementation based on the algorithm outlined by
 * B P Welford.
 */
public class Welford {

    private static double EPSILON = 0.000001;

    @Test
    public void noValues() {
        RunningStatistics z1 = new RunningStatistics();
        assertEquals("no values, mean", 0.0, z1.mean(), EPSILON);
        assertEquals("no values, sample var", Double.NaN, z1.var(), EPSILON);
        assertEquals("no values, var", Double.NaN, z1.popVar(), EPSILON);
        assertEquals("no values, sample std", Double.NaN, z1.std(), EPSILON);
        assertEquals("no values, std", Double.NaN, z1.popStd(), EPSILON);

        RunningStatistics z2 = new RunningStatistics(0.0);
        assertEquals("zero, mean", 0.0, z2.mean(), EPSILON);
        assertEquals("zero, sample var", Double.NaN, z2.var(), EPSILON);
        assertEquals("zero, var", 0.0, z2.popVar(), EPSILON);
        assertEquals("zero, sample std", Double.NaN, z2.std(), EPSILON);
        assertEquals("zero, std", 0.0, z2.popStd(), EPSILON);

        RunningStatistics z3 = new RunningStatistics(0.0, 0.0, 0.0, 0.0, 0.0);
        assertEquals("many zeros, mean", 0.0, z3.mean(), EPSILON);
        assertEquals("many zeros, sample var", 0.0, z3.var(), EPSILON);
        assertEquals("many zeros, var", 0.0, z3.popVar(), EPSILON);
        assertEquals("many zeros, sample std", 0.0, z3.std(), EPSILON);
        assertEquals("many zeros, std", 0.0, z3.popStd(), EPSILON);
    }

    @Test
    public void presetValues1() {
        double d[] = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 20.0 };
        RunningStatistics rs = new RunningStatistics(d);
        assertEquals("mean of vals: " + stringify(d),
                5.8571428571428568, rs.mean(), EPSILON);
        assertEquals("sample var of vals: " + stringify(d),
                41.80952380952381, rs.var(), EPSILON);
        assertEquals("var of vals: " + stringify(d),
                35.836734693877546, rs.popVar(), EPSILON);
        assertEquals("sample std of vals: " + stringify(d),
                6.4660284417503, rs.std(), EPSILON);
        assertEquals("std of vals: " + stringify(d),
                5.9863790970734172, rs.popStd(), EPSILON);
     }

    @Test
    public void presetValues2() {
        double d[] = { 1.0, 2.0, 3.0 };
        RunningStatistics rs = new RunningStatistics(d);
        assertEquals("mean of vals: " + stringify(d),
                2.0, rs.mean(), EPSILON);
        assertEquals("sample var of vals: " + stringify(d),
                1.0, rs.var(), EPSILON);
        assertEquals("var of vals: " + stringify(d),
                0.66666666666666663, rs.popVar(), EPSILON);
        assertEquals("sample std of vals: " + stringify(d),
                1.0, rs.std(), EPSILON);
        assertEquals("std of vals: " + stringify(d),
                0.81649658092772603, rs.popStd(), EPSILON);
     }

    @Test
    public void presetValues3() {
        double d[] = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
        RunningStatistics rs = new RunningStatistics(d);
        assertEquals("mean of vals: " + stringify(d),
                3.5, rs.mean(), EPSILON);
        assertEquals("sample var of vals: " + stringify(d),
                3.5, rs.var(), EPSILON);
        assertEquals("var of vals: " + stringify(d),
                2.9166666666666665, rs.popVar(), EPSILON);
        assertEquals("sample std of vals: " + stringify(d),
                1.8708286933869707, rs.std(), EPSILON);
        assertEquals("std of vals: " + stringify(d),
                1.707825127659933, rs.popStd(), EPSILON);
     }

    @Test
    public void presetValues4() {
        double d[] = { 1.0, 2.0, 4.0 };
        RunningStatistics rs = new RunningStatistics(d);
        assertEquals("mean of vals: " + stringify(d),
                2.3333333333333333, rs.mean(), EPSILON);
        assertEquals("sample var of vals: " + stringify(d),
                2.3333333333333333, rs.var(), EPSILON);
        assertEquals("var of vals: " + stringify(d),
                1.5555555555555555, rs.popVar(), EPSILON);
        assertEquals("sample std of vals: " + stringify(d),
                1.5275252316519465, rs.std(), EPSILON);
        assertEquals("std of vals: " + stringify(d),
                1.247219128924647, rs.popStd(), EPSILON);
    }

    @Test
    public void presetValues5() {
        double d[] = { 1.0 };
        RunningStatistics rs = new RunningStatistics(d);
        assertEquals("mean of vals: " + stringify(d),
                1.0, rs.mean(), EPSILON);
        assertEquals("sample var of vals: " + stringify(d),
                Double.NaN, rs.var(), EPSILON);
        assertEquals("var of vals: " + stringify(d),
                0.0, rs.popVar(), EPSILON);
        assertEquals("sample std of vals: " + stringify(d),
                Double.NaN, rs.std(), EPSILON);
        assertEquals("std of vals: " + stringify(d),
                0.0, rs.popStd(), EPSILON);
    }

    private String stringify(double[] ds) {
        String s = "";
        for (double d : ds) {
            s += d + " ";
        }
        return s;
    }
}
