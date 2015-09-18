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

import java.util.Random;

import galileo.stat.RunningStatistics;
import galileo.util.PerformanceTimer;

/**
 * Benchmarks the Welford running statistics tracker.
 */
public class WelfordBench {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: WelfordBench iterations");
            System.exit(1);
        }

        int iters = Integer.parseInt(args[0]);

        testUpdate(iters);
        testRemove(iters);
        testMean(iters);
        testSTD(iters);
        testMerge(iters);
        testT(iters);
    }

    private static void testUpdate(int iters) {
        /* Generate our incoming samples */
        double[] samples = generateSamples(iters);

        RunningStatistics rs = new RunningStatistics();
        PerformanceTimer pt = new PerformanceTimer("welford-update");
        for (int j = 0; j < iters; ++j) {
            pt.start();
            rs.put(samples[j]);
            pt.stopAndPrint();
        }
    }

    private static void testMean(int iters) {
        double[] samples = generateSamples(iters);
        RunningStatistics rs = new RunningStatistics();
        PerformanceTimer meanpt = new PerformanceTimer("welford-mean");
        for (int j = 0; j < iters; ++j) {
            rs.put(samples[j]);
            meanpt.start();
            rs.mean();
            meanpt.stopAndPrint();
        }
    }

    private static void testSTD(int iters) {
        double[] samples = generateSamples(iters);

        RunningStatistics rs = new RunningStatistics();
        PerformanceTimer stdpt = new PerformanceTimer("welford-std");
        for (int j = 0; j < iters; ++j) {
            rs.put(samples[j]);
            stdpt.start();
            rs.std();
            stdpt.stopAndPrint();
        }
    }

    private static void testMerge(int iters) {
        double[] samples = generateSamples(iters);

        RunningStatistics rs1 = new RunningStatistics();
        RunningStatistics rs2 = new RunningStatistics();
        PerformanceTimer mergept = new PerformanceTimer("welford-merge");
        for (int j = 0; j < iters; ++j) {
            rs1.put(samples[j]);
            mergept.start();
            rs2.merge(rs1);
            mergept.stopAndPrint();
        }
    }

    private static void testT(int iters) {
        for (int i = 0; i < iters; ++i) {
            double[] samples1 = generateSamples(100000);
            double[] samples2 = generateSamples(100000);
            RunningStatistics rs1 = new RunningStatistics();
            RunningStatistics rs2 = new RunningStatistics();

            PerformanceTimer pt = new PerformanceTimer("welford-T");
            pt.start();

            rs1.put(samples1);
            rs2.put(samples2);
            RunningStatistics.welchT(rs1, rs2);
            pt.stopAndPrint();
        }
    }

    private static void testRemove(int iters) {
        double[] samples = generateSamples(iters);
        RunningStatistics rs = new RunningStatistics();
        for (double sample : samples) {
            rs.put(sample);
        }
        PerformanceTimer pt = new PerformanceTimer("welford-remove");
        for (int j = 0; j < iters; ++j) {
            pt.start();
            rs.remove(samples[j]);
            pt.stopAndPrint();
        }
    }

    protected static double[] generateSamples(int numSamples) {
        Random rand = new Random();
        double[] samples = new double[numSamples];
        for (int j = 0; j < numSamples; ++j) {
            samples[j] = rand.nextDouble();
        }
        return samples;
    }
}
