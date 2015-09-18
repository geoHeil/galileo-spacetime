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

import galileo.stat.RunningStatistics2D;
import galileo.util.PerformanceTimer;

/**
 * Benchmarks the Welford running statistics tracker.
 */
public class WelfordBench2D {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: WelfordBench2D iterations");
            System.exit(1);
        }

        int iters = Integer.parseInt(args[0]);

        testUpdate(iters);
        testR(iters);
        testR2(iters);
        testPredict(iters);
        testMerge(iters);
    }

    private static void testUpdate(int iters) {
        /* Generate our incoming samples */
        double[] xSamples = WelfordBench.generateSamples(iters);
        double[] ySamples = WelfordBench.generateSamples(iters);

        RunningStatistics2D rs = new RunningStatistics2D();
        PerformanceTimer pt = new PerformanceTimer("welford2d-update");
        for (int j = 0; j < iters; ++j) {
            pt.start();
            rs.put(xSamples[j], ySamples[j]);
            pt.stopAndPrint();
        }
    }

    private static void testR(int iters) {
        double[] xSamples = WelfordBench.generateSamples(iters);
        double[] ySamples = WelfordBench.generateSamples(iters);
        RunningStatistics2D rs = new RunningStatistics2D();
        PerformanceTimer pt = new PerformanceTimer("welford2d-r");
        for (int i = 0; i < iters; ++i) {
            rs.put(xSamples[i], ySamples[i]);
            pt.start();
            rs.r();
            pt.stopAndPrint();
        }
    }

    private static void testR2(int iters) {
        double[] xSamples = WelfordBench.generateSamples(iters);
        double[] ySamples = WelfordBench.generateSamples(iters);
        RunningStatistics2D rs = new RunningStatistics2D();
        PerformanceTimer pt = new PerformanceTimer("welford2d-r2");
        for (int i = 0; i < iters; ++i) {
            rs.put(xSamples[i], ySamples[i]);
            pt.start();
            rs.r2();
            pt.stopAndPrint();
        }
    }

    private static void testPredict(int iters) {
        double[] xSamples = WelfordBench.generateSamples(iters);
        double[] ySamples = WelfordBench.generateSamples(iters);
        RunningStatistics2D rs = new RunningStatistics2D();
        for (int i = 0; i < iters; ++i) {
            rs.put(xSamples[i], ySamples[i]);
        }

        PerformanceTimer pt = new PerformanceTimer("welford2d-predict");
        double[] inputs = WelfordBench.generateSamples(iters);
        for (int i = 0; i < iters; ++i) {
            pt.start();
            rs.predict(inputs[i]);
            pt.stopAndPrint();
        }
    }


    private static void testMerge(int iters) {
        double[] xSamples = WelfordBench.generateSamples(iters);
        double[] ySamples = WelfordBench.generateSamples(iters);

        RunningStatistics2D rs1 = new RunningStatistics2D();
        RunningStatistics2D rs2 = new RunningStatistics2D();
        PerformanceTimer mergept = new PerformanceTimer("welford2d-merge");
        for (int j = 0; j < iters; ++j) {
            rs1.put(xSamples[j], ySamples[j]);
            mergept.start();
            rs2.merge(rs1);
            mergept.stopAndPrint();
        }
    }
}
