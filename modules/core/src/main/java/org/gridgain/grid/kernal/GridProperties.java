/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import java.io.*;
import java.util.*;

/**
 * GridGain properties holder.
 */
class GridProperties {
    /** Properties file path. */
    private static final String FILE_PATH = "gridgain.properties";

    /** Enterprise properties file path. */
    private static final String ENT_FILE_PATH = "gridgain-ent.properties";

    /** Properties. */
    private static final Properties PROPS;

    static {
        PROPS = new Properties();

        readProperties(FILE_PATH, true);
        readProperties(ENT_FILE_PATH, false);
    }

    /**
     * @param path Path.
     * @param throwExc Flag indicating whether to throw an exception or not.
     */
    private static void readProperties(String path, boolean throwExc) {
        InputStream is = GridProductImpl.class.getClassLoader().getResourceAsStream(path);

        if (is == null) {
            if (throwExc)
                throw new RuntimeException("Cannot find '" + path + "' file.");
            else
                return;
        }

        try {
            PROPS.load(is);
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot read '" + path + "' file.", e);
        }
    }

    /**
     * Gets property value.
     *
     * @param key Property key.
     * @return Property value.
     */
    public static String get(String key) {
        return PROPS.getProperty(key, "");
    }
}
