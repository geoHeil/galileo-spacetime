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

package galileo.logging;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.text.DateFormat;

import java.util.Date;

import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * The basic Galileo logging formatter, based on
 * {@link java.util.logging.SimpleFormatter}.
 * This formatter only prints detailed information if an important message
 * (WARNING, SEVERE) is logged or an exception is provided.  Otherwise, messages
 * are written without additional details.
 *
 * @author malensek
 */
public class GalileoFormatter extends Formatter {

    private DateFormat dateFormat;
    private final String lineSep = System.getProperty("line.separator");

    private Date date = new Date();

    public GalileoFormatter() {
        super();
    }

    @Override
    public String format(LogRecord record) {
        StringBuffer sb = new StringBuffer(100);

        if (dateFormat == null) {
            dateFormat = DateFormat.getDateTimeInstance();
        }

        Throwable thrown = record.getThrown();

        if (record.getLevel().intValue() > Level.INFO.intValue() ||
                thrown != null) {
            date.setTime(record.getMillis());

            sb.append(dateFormat.format(date));
            sb.append(' ');
            if (record.getSourceClassName() != null) {
                sb.append(record.getSourceClassName());
            } else {
                sb.append("logger=" + record.getLoggerName());
            }
            if (record.getSourceMethodName() != null) {
                sb.append(' ');
                sb.append(record.getSourceMethodName());
            }
            sb.append(lineSep);

            sb.append(record.getLevel());
            sb.append(": ");
        }

        sb.append(formatMessage(record));
        sb.append(lineSep);

        if (thrown != null) {
            StringWriter writer = new StringWriter();
            PrintWriter printer = new PrintWriter(writer, true);
            thrown.printStackTrace(printer);
            sb.append(writer.toString());
            printer.close();
        }

        return sb.toString();
    }
}
