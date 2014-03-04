/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.gar;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Imported class which should be placed in JAR file in GAR/lib folder.
 * Loads message resource file via class loader. See {@code GridGarHelloWorldExample} for more details.
 *
 * @author link
 * @version 1.5
 */
public class GridGarHelloWorldBean {
    /** */
    public static final String RESOURCE = "org/gridgain/examples/gar/gar-example.properties";

    /**
     * Gets keyed message.
     *
     * @param key Message key.
     * @return Keyed message.
     */
    @Nullable
    public String getMessage(String key) {
        InputStream in = null;

        try {
            in = getClass().getClassLoader().getResourceAsStream(RESOURCE);

            Properties props = new Properties();

            props.load(in);

            return props.getProperty(key);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally{
            U.close(in, null);
        }

        return null;
    }
}
