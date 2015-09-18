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

package galileo.config;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides general system configuration information.  The settings contained
 * within are guaranteed to not change during execution unless the reload()
 * method is called explicitly.
 *
 * @author malensek
 */
public class SystemConfig {

    private static final Logger logger = Logger.getLogger("galileo");

    private final static String networkDir = "network";

    /** Storage root */
    private static String rootDir;

    /** Configuration directory */
    private static String confDir;

    /** Galileo install directory (binaries, libraries) */
    private static String homeDir;

    /**
     * Retrieves the system root directory.  This directory is where Galileo
     * stores files.
     */
    public static String getRootDir() {
        return rootDir;
    }

    /**
     * Retrieves the system configuration directory, which contains all Galileo
     * configuration directives.
     */
    public static String getConfDir() {
        return confDir;
    }

    /**
     * Retrieves the Galileo installation directory, which contains the
     * binaries, scripts, and libraries required to run Galileo.
     */
    public static String getInstallDir() {
        return homeDir;
    }

    /**
     * Retrives the network configuration directory, which describes the DHT
     * groups and the nodes assigned to them.
     */
    public static String getNetworkConfDir() {
        return confDir + "/" + networkDir;
    }

    /**
     * Reloads the Galileo system configuration.
     */
    public static void reload() {
        logger.log(Level.CONFIG, "Reloading system configuration");
        load();
    }

    /**
     * Loads all system configuration settings.
     */
    private static void load() {
        String home = System.getenv("GALILEO_HOME");
        if (home == null) {
            home = System.getProperty("installDirectory", ".");
        }
        homeDir = home;

        String storageDir = System.getenv("GALILEO_ROOT");
        if (storageDir == null) {
            storageDir = System.getProperty("storageDirectory", home);
        }
        rootDir = storageDir;

        String configDir = System.getenv("GALILEO_CONF");
        if (configDir == null) {
            configDir = System.getProperty("configDirectory",
                    rootDir + "/config");
        }
        confDir = configDir;

    }

    static {
        load();
    }
}
