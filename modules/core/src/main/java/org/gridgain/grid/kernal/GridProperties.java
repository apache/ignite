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

    /** Properties. */
    private static final Properties PROPS;

    static {
        PROPS = new Properties();

        try {
            PROPS.load(GridProductImpl.class.getClassLoader().getResourceAsStream(FILE_PATH));
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot find '" + FILE_PATH + "' file.", e);
        }
    }

    /**
     * Gets property value.
     *
     * @param key Property key.
     * @return Property value.
     */
    public static String get(String key) {
        return PROPS.getProperty(key);
    }
}
