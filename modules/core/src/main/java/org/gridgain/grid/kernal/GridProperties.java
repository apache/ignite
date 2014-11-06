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
public class GridProperties {
    /** Properties file path. */
    private static final String FILE_PATH = "gridgain.properties";

    /** Enterprise properties file path. */
    private static final String ENT_FILE_PATH = "gridgain-ent.properties";

    /** Properties. */
    private static final Properties PROPS;

    /**
     *
     */
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
        try (InputStream is = GridProductImpl.class.getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                if (throwExc)
                    throw new RuntimeException("Failed to find properties file: " + path);
                else
                    return;
            }

            PROPS.load(is);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read properties file: " + path, e);
        }
    }

    /**
     * Gets property value.
     *
     * @param key Property key.
     * @return Property value (possibly empty string, but never {@code null}).
     */
    public static String get(String key) {
        return PROPS.getProperty(key, "");
    }

    /**
     *
     */
    private GridProperties() {
        // No-op.
    }
}
