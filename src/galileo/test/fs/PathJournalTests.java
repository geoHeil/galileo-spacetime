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

package galileo.test.fs;

import galileo.dataset.feature.Feature;
import galileo.fs.PathJournal;
import galileo.graph.FeaturePath;

import java.io.File;

import org.junit.Test;

public class PathJournalTests {

    private static String journal = "/tmp/pathjournal";
    private static String index = "/tmp/pathjournal.index";

    public PathJournalTests() {
        removeJournal();
    }

    private void removeJournal() {
        new File(journal).delete();
        new File(index).delete();
    }

    @Test
    public void testCreation() throws Exception {
        removeJournal();

        FeaturePath<String> fp = new FeaturePath<>();
        fp.add(new Feature("humidity", 24.3f));
        fp.add(new Feature("temperature", 4.1f));
        fp.add(new Feature("wind", 8.0f));
        fp.add(new Feature("snow", 0.32f));
        fp.addPayload("/a/b/c/d");

        FeaturePath<String> fp2 = new FeaturePath<>();
        fp.add(new Feature("humidity", 84.3f));
        fp.add(new Feature("temperature", 43.1f));
        fp.add(new Feature("wind", 1.0f));
        fp.add(new Feature("snow", 0.0f));
        fp.addPayload("/a/b/c/test");

        PathJournal pj = new PathJournal("/tmp/pathjournal");
        pj.start();
        pj.persistPath(fp);
        pj.persistPath(fp2);

        System.out.println("=======");
    }
}
