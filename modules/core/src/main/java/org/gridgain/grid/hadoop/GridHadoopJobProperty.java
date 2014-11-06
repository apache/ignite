/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.jetbrains.annotations.*;

/**
 * Enumeration of optional properties supported by GridGain for Apache Hadoop.
 */
public enum GridHadoopJobProperty {
    /**
     * Initial size for hashmap which stores output of mapper and will be used as input of combiner.
     * <p>
     * Setting it right allows to avoid rehashing.
     */
    COMBINER_HASHMAP_SIZE,

    /**
     * Initial size for hashmap which stores output of mapper or combiner and will be used as input of reducer.
     * <p>
     * Setting it right allows to avoid rehashing.
     */
    PARTITION_HASHMAP_SIZE,

    /**
     * Specifies number of concurrently running mappers for external execution mode.
     * <p>
     * If not specified, defaults to {@code Runtime.getRuntime().availableProcessors()}.
     */
    EXTERNAL_CONCURRENT_MAPPERS,

    /**
     * Specifies number of concurrently running reducers for external execution mode.
     * <p>
     * If not specified, defaults to {@code Runtime.getRuntime().availableProcessors()}.
     */
    EXTERNAL_CONCURRENT_REDUCERS,

    /**
     * Delay in milliseconds after which GridGain server will reply job status.
     */
    JOB_STATUS_POLL_DELAY,

    /**
     * Size in bytes of single memory page which will be allocated for data structures in shuffle.
     * <p>
     * By default is {@code 32 * 1024}.
     */
    SHUFFLE_OFFHEAP_PAGE_SIZE,

    /**
     * If set to {@code true} then input for combiner will not be sorted by key.
     * Internally hash-map will be used instead of sorted one, so {@link Object#equals(Object)}
     * and {@link Object#hashCode()} methods of key must be implemented consistently with
     * comparator for that type. Grouping comparator is not supported if this setting is {@code true}.
     * <p>
     * By default is {@code false}.
     */
    SHUFFLE_COMBINER_NO_SORTING,

    /**
     * If set to {@code true} then input for reducer will not be sorted by key.
     * Internally hash-map will be used instead of sorted one, so {@link Object#equals(Object)}
     * and {@link Object#hashCode()} methods of key must be implemented consistently with
     * comparator for that type. Grouping comparator is not supported if this setting is {@code true}.
     * <p>
     * By default is {@code false}.
     */
    SHUFFLE_REDUCER_NO_SORTING;

    /** */
    private final String ptyName;

    /**
     *
     */
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
     * @param jobInfo Job info.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static String get(GridHadoopJobInfo jobInfo, GridHadoopJobProperty pty, @Nullable String dflt) {
        String res = jobInfo.property(pty.propertyName());

        return res == null ? dflt : res;
    }

    /**
     * @param jobInfo Job info.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static int get(GridHadoopJobInfo jobInfo, GridHadoopJobProperty pty, int dflt) {
        String res = jobInfo.property(pty.propertyName());

        return res == null ? dflt : Integer.parseInt(res);
    }

    /**
     * @param jobInfo Job info.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static boolean get(GridHadoopJobInfo jobInfo, GridHadoopJobProperty pty, boolean dflt) {
        String res = jobInfo.property(pty.propertyName());

        return res == null ? dflt : Boolean.parseBoolean(res);
    }
}
