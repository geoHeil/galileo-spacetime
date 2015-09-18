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

package galileo.util;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 * A very simple performance timer implementation using System.nanoTime() that
 * writes its results to a file.
 *
 * This class can record multiple samples (time intervals), print them, etc.
 * As with any IO-based class, the LoggedPerformanceTimer should be closed to
 * flush its contents to disk.
 *
 * @author malensek
 */
public class LoggedPerformanceTimer extends PerformanceTimer
    implements Closeable {

    private PrintWriter writer;

    /**
     * Creates a logged performance timer with the given log file name.
     */
    public LoggedPerformanceTimer(String logFileName)
    throws FileNotFoundException {
        super();
        writer = new PrintWriter(logFileName);
    }

    /**
     * Creates a logged performance timer with the given log file name and
     * timer name.
     */
    public LoggedPerformanceTimer(String fileName, String timerName)
    throws FileNotFoundException {
        this(fileName);
        this.name = timerName;
    }

    @Override
    public void stop() {
        PerformanceSample sample = samples.peekFirst();
        super.stop();
        writer.println(sample);
    }

    @Override
    public void close() {
        writer.close();
    }
}
