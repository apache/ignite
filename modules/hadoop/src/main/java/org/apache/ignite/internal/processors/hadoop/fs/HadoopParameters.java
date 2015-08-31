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

package org.apache.ignite.internal.processors.hadoop.fs;

/**
 * This class lists parameters that can be specified in Hadoop configuration.
 * Hadoop configuration can be specified in {@code core-site.xml} file
 * or passed to map-reduce task directly when using Hadoop driver for IGFS file system:
 * <ul>
 *     <li>
 *         {@code fs.igfs.[name].open.sequential_reads_before_prefetch} - this parameter overrides
 *         the one specified in {@link org.apache.ignite.configuration.FileSystemConfiguration#getSequentialReadsBeforePrefetch()}
 *         IGFS data node configuration property.
 *     </li>
 *     <li>
 *         {@code fs.igfs.[name].log.enabled} - specifies whether IGFS sampling logger is enabled. If
 *         {@code true}, then all file system operations will be logged to a file.
 *     </li>
 *     <li>{@code fs.igfs.[name].log.dir} - specifies log directory where sampling log files should be placed.</li>
 *     <li>
 *         {@code fs.igfs.[name].log.batch_size} - specifies how many log entries are accumulated in a batch before
 *         it gets flushed to log file. Higher values will imply greater performance, but will increase delay
 *         before record appears in the log file.
 *     </li>
 *     <li>
 *         {@code fs.igfs.[name].colocated.writes} - specifies whether written files should be colocated on data
 *         node to which client is connected. If {@code true}, file will not be distributed and will be written
 *         to a single data node. Default value is {@code true}.
 *     </li>
 *     <li>
 *         {@code fs.igfs.prefer.local.writes} - specifies whether file preferably should be written to
 *         local data node if it has enough free space. After some time it can be redistributed across nodes though.
 *     </li>
 * </ul>
 * Where {@code [name]} is file system endpoint which you specify in file system URI authority part. E.g. in
 * case your file system URI is {@code igfs://127.0.0.1:10500} then {@code name} will be {@code 127.0.0.1:10500}.
 * <p>
 * Sample configuration that can be placed to {@code core-site.xml} file:
 * <pre name="code" class="xml">
 *     &lt;property&gt;
 *         &lt;name&gt;fs.igfs.127.0.0.1:10500.log.enabled&lt;/name&gt;
 *         &lt;value&gt;true&lt;/value&gt;
 *     &lt;/property&gt;
 *     &lt;property&gt;
 *         &lt;name&gt;fs.igfs.127.0.0.1:10500.log.dir&lt;/name&gt;
 *         &lt;value&gt;/home/apache/ignite/log/sampling&lt;/value&gt;
 *     &lt;/property&gt;
 *     &lt;property&gt;
 *         &lt;name&gt;fs.igfs.127.0.0.1:10500.log.batch_size&lt;/name&gt;
 *         &lt;value&gt;16&lt;/value&gt;
 *     &lt;/property&gt;
 * </pre>
 * Parameters could also be specified per mapreduce job, e.g.
 * <pre name="code" class="bash">
 * hadoop jar myjarfile.jar MyMapReduceJob -Dfs.igfs.open.sequential_reads_before_prefetch=4
 * </pre>
 * If you want to use these parameters in code, then you have to substitute you file system name in it. The easiest
 * way to do that is {@code String.format(PARAM_IGFS_COLOCATED_WRITES, [name])}.
 */
public class HadoopParameters {
    /** Parameter name for control over file colocation write mode. */
    public static final String PARAM_IGFS_COLOCATED_WRITES = "fs.igfs.%s.colocated.writes";

    /** Parameter name for custom sequential reads before prefetch value. */
    public static final String PARAM_IGFS_SEQ_READS_BEFORE_PREFETCH =
        "fs.igfs.%s.open.sequential_reads_before_prefetch";

    /** Parameter name for client logger directory. */
    public static final String PARAM_IGFS_LOG_DIR = "fs.igfs.%s.log.dir";

    /** Parameter name for log batch size. */
    public static final String PARAM_IGFS_LOG_BATCH_SIZE = "fs.igfs.%s.log.batch_size";

    /** Parameter name for log enabled flag. */
    public static final String PARAM_IGFS_LOG_ENABLED = "fs.igfs.%s.log.enabled";

    /** Parameter name for prefer local writes flag. */
    public static final String PARAM_IGFS_PREFER_LOCAL_WRITES = "fs.igfs.prefer.local.writes";
}