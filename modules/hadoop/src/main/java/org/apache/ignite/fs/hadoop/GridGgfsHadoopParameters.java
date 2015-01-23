/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.fs.hadoop;

/**
 * This class lists parameters that can be specified in Hadoop configuration.
 * Hadoop configuration can be specified in {@code core-site.xml} file
 * or passed to map-reduce task directly when using Hadoop driver for GGFS file system:
 * <ul>
 *     <li>
 *         {@code fs.ggfs.[name].open.sequential_reads_before_prefetch} - this parameter overrides
 *         the one specified in {@link org.apache.ignite.fs.IgniteFsConfiguration#getSequentialReadsBeforePrefetch()}
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
 *     <li>
 *         {@code fs.ggfs.prefer.local.writes} - specifies whether file preferably should be written to
 *         local data node if it has enough free space. After some time it can be redistributed across nodes though.
 *     </li>
 * </ul>
 * Where {@code [name]} is file system endpoint which you specify in file system URI authority part. E.g. in
 * case your file system URI is {@code ggfs://127.0.0.1:10500} then {@code name} will be {@code 127.0.0.1:10500}.
 * <p>
 * Sample configuration that can be placed to {@code core-site.xml} file:
 * <pre name="code" class="xml">
 *     &lt;property&gt;
 *         &lt;name&gt;fs.ggfs.127.0.0.1:10500.log.enabled&lt;/name&gt;
 *         &lt;value&gt;true&lt;/value&gt;
 *     &lt;/property&gt;
 *     &lt;property&gt;
 *         &lt;name&gt;fs.ggfs.127.0.0.1:10500.log.dir&lt;/name&gt;
 *         &lt;value&gt;/home/gridgain/log/sampling&lt;/value&gt;
 *     &lt;/property&gt;
 *     &lt;property&gt;
 *         &lt;name&gt;fs.ggfs.127.0.0.1:10500.log.batch_size&lt;/name&gt;
 *         &lt;value&gt;16&lt;/value&gt;
 *     &lt;/property&gt;
 * </pre>
 * Parameters could also be specified per mapreduce job, e.g.
 * <pre name="code" class="bash">
 * hadoop jar myjarfile.jar MyMapReduceJob -Dfs.ggfs.open.sequential_reads_before_prefetch=4
 * </pre>
 * If you want to use these parameters in code, then you have to substitute you file system name in it. The easiest
 * way to do that is {@code String.format(PARAM_GGFS_COLOCATED_WRITES, [name])}.
 */
public class GridGgfsHadoopParameters {
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

    /** Parameter name for prefer local writes flag. */
    public static final String PARAM_GGFS_PREFER_LOCAL_WRITES = "fs.ggfs.prefer.local.writes";
}
