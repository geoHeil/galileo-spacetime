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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.util.FileNames;
import galileo.util.Pair;

/**
 * Reads Galileo DHT information from disk.  The on-disk format for a network
 * setup includes a directory with several *.group files within.  Each line in a
 * group file contains a hostname:port pair, although a port number is not
 * required if the server in question is using the default port number.
 *
 * @author malensek
 */
public class NetworkConfig {

    private static final Logger logger = Logger.getLogger("galileo");

    public static final int DEFAULT_PORT = 5634;
    public static final String GROUP_EXT = "group";

    /**
     * Reads a network description directory from disk.
     *
     * @param directory full path name of the network description directory.
     *
     * @return NetworkInfo containing the network information read from the
     * directory.
     */
    public static NetworkInfo readNetworkDescription(String directory)
    throws FileNotFoundException, IOException {

        NetworkInfo network = new NetworkInfo();

        File dir = new File(directory);
        if (!dir.exists()) {
            throw new FileNotFoundException("Could not find network " +
                    "configuration directory");
        }
        File[] groupFiles = dir.listFiles();
        Arrays.sort(groupFiles);
        for (File file : groupFiles) {

            Pair<String, String> pair = FileNames.splitExtension(file);
            String ext = pair.b;

            if (ext.toLowerCase().equals(GROUP_EXT)) {
                GroupInfo group = readGroupFile(file);
                network.addGroup(group);
            }
        }

        return network;
    }

    /**
     * Read host:port pairs from a group description file (*.group).
     *
     * @param file File containing the group members.
     *
     * @return GroupInfo containing the hosts read from file.
     */
    public static GroupInfo readGroupFile(File file)
    throws IOException {
        Pair<String, String> pair = FileNames.splitExtension(file);
        String groupName = pair.a;

        GroupInfo group = new GroupInfo(groupName);

        FileReader fReader = new FileReader(file);

        BufferedReader reader = new BufferedReader(fReader);
        int lineNum = 0;
        String line;

        /* A line looks something like hostname:port */
        while ((line = reader.readLine()) != null) {
            ++lineNum;
            line = line.trim().replaceAll("\\s","");
            if (line.startsWith("#") || line.equals("")) {
                continue;
            }

            String[] hostInfo = line.split(":", 2);
            String nodeName = hostInfo[0];
            if (nodeName.equals("")) {
                logger.warning("Could not determine StorageNode " +
                        "hostname for group '" + groupName + "' on line " +
                        lineNum + "; ignoring entry.");

                continue;
            }

            if (hostInfo.length <= 1) {
                /* No port specified; use the default port. */
                NodeInfo node = new NodeInfo(nodeName.toLowerCase(), DEFAULT_PORT);
                group.addNode(node);
            } else {
                /* A port, or list of several comma-separated ports, has been
                 * specified. */
                String portStr = hostInfo[1];
                String ports[] = portStr.split(",");
                for (String portEntry : ports) {
                    try {
                        int port = Integer.parseInt(portEntry);
                        group.addNode(new NodeInfo(nodeName.toLowerCase(), port));
                    } catch (NumberFormatException e) {
                        logger.log(Level.WARNING, "Could not parse " +
                                "StorageNode port number on line " + lineNum +
                                ";  ignoring entry.", e);
                    }
                }
            }
        }
        reader.close();

        return group;
    }
}
