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

package galileo.dht;

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class provides a very simple way to inform users of status information
 * at a glance.  For more sophisticated debugging, the user should consult
 * relevant log files; however, this simple status log can provide a starting
 * point.  The StatusLine should be updated infrequently; each call will open
 * and close the status file unless a standard out based StatusLine is being
 * used.
 *
 * @author malensek
 */
public class StatusLine {

    private static final Logger logger = Logger.getLogger("galileo");

    private String fileName;
    private boolean useStdout = false;
    private boolean active = true;

    /**
     * Creates a new StatusLine that will be written to standard out.
     */
    public StatusLine() {
        useStdout = true;
    }

    /**
     * Creates a new StatusLine that will be written to the given file name.
     *
     * @param fileName status file
     */
    public StatusLine(String fileName) {
        this.fileName = fileName;

        PrintWriter writer = null;
        try {
            writer = new PrintWriter(fileName);
            writer.println("");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Status file is not writable! "
                    + "Falling back to standard out for status messages.");
            useStdout = true;
        } finally {
            if (writer != null) {
                writer.close();
            }
            writer = null;
        }
    }

    /**
     * Sets the current status.  Previous status information will be replaced.
     *
     * @param status the new status line to write to disk immediately.
     */
    public void set(String status) {
        if (!active) {
            return;
        }

        if (useStdout) {
            System.out.println(status);
            return;
        }

        try {
            PrintWriter writer = new PrintWriter(fileName);
            writer.println(status);
            writer.close();
            writer = null;
        } catch (Exception e) {
            logger.log(Level.WARNING, "Could not update status line.", e);
        }
    }

    /**
     * Deactivates the status line and removes the text file (if any) from disk.
     */
    public void close() {
        if (!useStdout) {
            File line = new File(fileName);
            line.delete();
        }
        active = false;
    }
}
