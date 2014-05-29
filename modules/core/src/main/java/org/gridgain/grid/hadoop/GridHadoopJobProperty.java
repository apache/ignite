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
     * Use single combiner for all mappers running in current process. By default is {@code false}.
     */
    SINGLE_COMBINER_FOR_ALL_MAPPERS,

    /**
     * Size in bytes of single memory page which will be allocated for data structures in shuffle.
     * <p>
     * By default is {@code 16 * 1024}.
     */
    SHUFFLE_OFFHEAP_PAGE_SIZE;

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

    /**
     * @param job Job.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static boolean get(GridHadoopJob job, GridHadoopJobProperty pty, boolean dflt) {
        String res = job.property(pty.propertyName());

        return res == null ? dflt : Boolean.parseBoolean(res);
    }
}
