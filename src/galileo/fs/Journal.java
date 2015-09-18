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

package galileo.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public final class Journal {

    private static final Journal instance = new Journal();

    private File journalFile;
    private FileReader reader;
    private BufferedReader bufferedReader;
    private FileWriter writer;
    private PrintWriter printWriter;


    public static Journal getInstance() {
        return instance;
    }

    public BufferedReader getReader() {
        return bufferedReader;
    }

    public void writeEntry(String entry)
    throws IOException {
        printWriter.println(entry);
        writer.flush();
    }

    public void close()
    throws IOException {
        printWriter.close();
    }

    public void setJournalPath(String path)
    throws IOException {
        journalFile = new File(path);

        writer = new FileWriter(journalFile, true);
        printWriter = new PrintWriter(writer);

        reader = new FileReader(journalFile);
        bufferedReader = new BufferedReader(reader);
    }
}
