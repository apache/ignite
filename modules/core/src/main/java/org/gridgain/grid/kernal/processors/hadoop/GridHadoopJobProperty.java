/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.hadoop.*;
import org.jetbrains.annotations.*;

/**
 * Enumeration of optional properties supported by GridGain for Apache Hadoop.
 */
public enum GridHadoopJobProperty {
    /** */
    COMBINER_HASHMAP_SIZE,

    /** */
    MAPPER_HASHMAP_SIZE;

    /** */
    private final String ptyName;

    GridHadoopJobProperty() {
        ptyName = "gridgain." + name().toLowerCase().replace('_', '.');
    }

    /**
     * @return Property name.
     */
    public String propertyName() {
        return ptyName;
    }

    /**
     * @param job Job.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static String get(GridHadoopJob job, GridHadoopJobProperty pty, @Nullable String dflt) {
        String res = job.property(pty.propertyName());

        return res == null ? dflt : res;
    }

    /**
     * @param job Job.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static int get(GridHadoopJob job, GridHadoopJobProperty pty, int dflt) {
        String res = job.property(pty.propertyName());

        return res == null ? dflt : Integer.parseInt(res);
    }
}
