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

package org.apache.ignite.yardstick.upload;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.beust.jcommander.Parameter;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.yardstick.IgniteBenchmarkArguments;
import org.jetbrains.annotations.Nullable;

/**
 * Represents command line arguments that are specific to upload benchmarks.
 *
 * @see IgniteBenchmarkArguments
 */
@SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
public class UploadBenchmarkArguments implements StreamerParams {
    /** Whither or not temporary disable Write Ahead Log during upload. */
    @Parameter(names = {"--disable-wal"},
        arity = 1,
        description = "Upload benchmark only: " +
            "turn off Write Ahead Log before data uploading " +
            "and turn it on again when upload is done.")
    private boolean disableWal = false;

    /**
     * Parameters for JDBC connection, that only uploads data.
     *
     * We can't just pass entire params string, due to yardstick, which relies on bash, has some troubles with escaping
     * ampersand character.
     */
    @Parameter(names = {"--sql-jdbc-params"},
        variableArity = true,
        description = "Upload benchmark only: " +
            "Additional url parameters (space separated key=value) for special JDBC connection that only uploads data. ")
    private List<String> uploadJdbcParams = Collections.emptyList();

    @Parameter(names = {"--sql-copy-packet-size"},
        description = "Upload benchmark only: use custom packet_size (in bytes) for copy command.")
    private Long copyPacketSize = null;

    @Parameter(names = {"--streamer-node-buf-size"},
        description = "Streamer benchmarks only: Set streamer's perNodeBufferSize property")
    private Integer streamerNodeBufSize = null;

    @Parameter(names = {"--streamer-node-par-ops"},
        description = "Streamer benchmarks only: Set streamer's perNodeParallelOperations property")
    private Integer streamerNodeParOps = null;

    @Parameter(names = {"--streamer-local-batch-size"},
        description = "Streamer benchmarks only: collect entries before passing to java streamer api." +
            "If set to 1, than entries will be passed directly.")
    private Integer streamerLocBatchSize = null;

    @Parameter(names = {"--streamer-allow-overwrite"}, arity = 1,
        description = "Streamer benchmarks only: set allowOverwrite streamer parameter.")
    private Boolean streamerAllowOverwrite = null;

    @Parameter(names = {"--streamer-ordered"}, arity = 1,
        description = "Streamer benchmarks only: set streamer ordered flag.")
    private boolean streamerOrdered = false;

    /** How many rows to upload during warmup. */
    @Parameter(names = {"--upload-warmup-rows"})
    private long warmupRowsCnt = 3_000_000;

    /** How many rows to upload during real test. */
    @Parameter(names = {"--upload-rows"})
    private long uploadRowsCnt = -1;

    /** How many rows to include in each batch ({@link BatchedInsertBenchmark} only). */
    @Parameter(names = {"--upload-jdbc-batch-size"})
    private long jdbcBatchSize = -1;

    /** Turn on streaming during upload. */
    @Parameter(names = {"--use-streaming"}, arity = 1,
        description = "Upload data in insert benchmarks in streaming mode")
    private boolean useStreaming = false;

    /** Number of secondary indexes to create before upload. Values can be from 0 up to 10. */
    @Parameter(names = {"--idx-count"})
    private int idxCnt = 0;

    /**
     * @return Switch wal.
     */
    public boolean disableWal() {
        return disableWal;
    }

    /** @return parameters for JDBC url. */
    public List<String> uploadJdbcParams() {
        return uploadJdbcParams;
    }

    /** @return packet_size value for copy command or {@code null} for default value. */
    @Nullable public Long copyPacketSize() {
        return copyPacketSize;
    }

    /**
     * @return Value for {@link IgniteDataStreamer#perNodeBufferSize(int)}.
     */
    @Override @Nullable public Integer streamerPerNodeBufferSize() {
        return streamerNodeBufSize;
    }

    /**
     * @return Value for {@link IgniteDataStreamer#perNodeParallelOperations(int)}.
     */
    @Override @Nullable public Integer streamerPerNodeParallelOperations() {
        return streamerNodeParOps;
    }

    /**
     * How many entries to collect before sending to java streamer api in either way: passing map to {@link
     * IgniteDataStreamer#addData(Map)}, or set STREAMING sql command parameter. <br/> If set to 1, {@link
     * IgniteDataStreamer#addData(Object, Object)} method will be used.
     */
    @Override @Nullable public Integer streamerLocalBatchSize() {
        return streamerLocBatchSize;
    }

    /**
     * Bypass corresponding parameter to streamer.
     */
    @Override @Nullable public Boolean streamerAllowOverwrite() {
        return streamerAllowOverwrite;
    }

    /**
     * Bypass corresponding parameter to streamer.
     */
    @Override public boolean streamerOrdered() {
        return streamerOrdered;
    }

    /**
     * See {@link #warmupRowsCnt}.
     */
    public long warmupRowsCnt() {
        return warmupRowsCnt;
    }

    /**
     * See {@link #uploadRowsCnt}.
     */
    public long uploadRowsCnt() {
        if (uploadRowsCnt < 0)
            throw new IllegalStateException("Upload rows count is not specified. Check arguments.");

        return uploadRowsCnt;
    }

    /**
     * See {@link #jdbcBatchSize}.
     */
    public long jdbcBatchSize() {
        if (jdbcBatchSize < 0)
            throw new IllegalStateException("JDBC batch size is not specified. Check arguments.");

        return jdbcBatchSize;
    }

    /**
     * See {@link #useStreaming}.
     */
    public boolean useStreaming() {
        return useStreaming;
    }

    /**
     * See {@link #idxCnt}.
     */
    public int indexesCount() {
        return idxCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(UploadBenchmarkArguments.class, this);
    }
}
