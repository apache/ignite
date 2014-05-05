/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.hadoop;

import org.gridgain.grid.ggfs.*;

/**
 * This class lists parameters that can be specified in Hadoop configuration.
 * Hadoop configuration can be specified in {@code core-site.xml} file
 * or passed to map-reduce task directly when using Hadoop driver for GGFS file system:
 * <ul>
 *     <li>
 *         {@code fs.ggfs.[name].endpoint.type} - this parameter specifies IPC endpoint type. In case not provided,
 *         "tcp" will be used for Windows platforms and "shmem" for all the other platforms.
 *     </li>
 *     <li>
 *         {@code fs.ggfs.[name].endpoint.host} - optional name of loopback interface to which IPC endpoint is bound.
 *         In case not provided, IPC endpoint will be bound to 127.0.0.1 IP address by default.
 *     </li>
 *     <li>
 *         {@code fs.ggfs.[name].endpoint.port} - this parameter specifies IPC endpoint port. In case not provided,
 *         default port {@link GridGgfsConfiguration#DFLT_IPC_PORT} will be used.
 *     </li>
 *     <li>
 *         {@code fs.ggfs.[name].open.sequential_reads_before_prefetch} - this parameter overrides
 *         the one specified in {@link GridGgfsConfiguration#getSequentialReadsBeforePrefetch()}
 *         GGFS data node configuration property.
 *     </li>
 *     <li>
 *         {@code fs.ggfs.[name].log.enabled} - specifies whether GGFS sampling logger is enabled. If
 *         {@code true}, then all file system operations will be logged to a file.
 *     </li>
 *     <li>{@code fs.ggfs.[name].log.dir} - specifies log directory where sampling log files should be placed.</li>
 *     <li>
 *         {@code fs.ggfs.[name].log.batch_size} - specifies how many log entries are accumulated in a batch before
 *         it gets flushed to log file. Higher values will imply greater performance, but will increase delay
 *         before record appears in the log file.
 *     </li>
 *     <li>
 *         {@code fs.ggfs.[name].colocated.writes} - specifies whether written files should be colocated on data
 *         node to which client is connected. If {@code true}, file will not be distributed and will be written
 *         to a single data node. Default value is {@code true}.
 *     </li>
 * </ul>
 * Where {@code [name]} is arbitrary file system name which you specify in file system URI authority part. E.g. in
 * case your file system URI is {@code ggfs://ipc} then {@code name} will be {@code ipc}.
 * <p>
 * Sample configuration that can be placed to {@code core-site.xml} file:
 * <pre name="code" class="xml">
 *     &lt;property&gt;
 *         &lt;name&gt;fs.ggfs.ipc.endpoint.type&lt;/name&gt;
 *         &lt;value&gt;shmem&lt;/value&gt;
 *     &lt;/property&gt;
 *     &lt;property&gt;
 *         &lt;name&gt;fs.ggfs.ipc.endpoint.port&lt;/name&gt;
 *         &lt;value&gt;10501&lt;/value&gt;
 *     &lt;/property&gt;
 *     &lt;property&gt;
 *         &lt;name&gt;fs.ggfs.ipc.log.enabled&lt;/name&gt;
 *         &lt;value&gt;true&lt;/value&gt;
 *     &lt;/property&gt;
 *     &lt;property&gt;
 *         &lt;name&gt;fs.ggfs.ipc.log.dir&lt;/name&gt;
 *         &lt;value&gt;/home/gridgain/log/sampling&lt;/value&gt;
 *     &lt;/property&gt;
 *     &lt;property&gt;
 *         &lt;name&gt;fs.ggfs.ipc.log.batch_size&lt;/name&gt;
 *         &lt;value&gt;16&lt;/value&gt;
 *     &lt;/property&gt;
 * </pre>
 * Parameters could also be specified per mapreduce job, e.g.
 * <pre name="code" class="bash">
 * hadoop jar myjarfile.jar MyMapReduceJob -Dfs.ggfs.open.sequential_reads_before_prefetch=4
 * </pre>
 * If you want to use these parameters in code, then you have to substitute you file system name in it. The easiest
 * way to do that is {@code String.format(PARAM_GGFS_ENDPOINT_TYPE, [name])}.
 */
public class GridGgfsHadoopParameters {
    /** Parameter name for IPC endpoint type. */
    public static final String PARAM_GGFS_ENDPOINT_TYPE = "fs.ggfs.%s.endpoint.type";

    /** Parameter name for IPC endpoint host. */
    public static final String PARAM_GGFS_ENDPOINT_HOST = "fs.ggfs.%s.endpoint.host";

    /** Parameter name for IPC endpoint port. */
    public static final String PARAM_GGFS_ENDPOINT_PORT = "fs.ggfs.%s.endpoint.port";

    /** Parameter name for control over file colocation write mode. */
    public static final String PARAM_GGFS_COLOCATED_WRITES = "fs.ggfs.%s.colocated.writes";

    /** Parameter name for custom sequential reads before prefetch value. */
    public static final String PARAM_GGFS_SEQ_READS_BEFORE_PREFETCH =
        "fs.ggfs.%s.open.sequential_reads_before_prefetch";

    /** Parameter name for client logger directory. */
    public static final String PARAM_GGFS_LOG_DIR = "fs.ggfs.%s.log.dir";

    /** Parameter name for log batch size. */
    public static final String PARAM_GGFS_LOG_BATCH_SIZE = "fs.ggfs.%s.log.batch_size";

    /** Parameter name for log enabled flag. */
    public static final String PARAM_GGFS_LOG_ENABLED = "fs.ggfs.%s.log.enabled";
}
