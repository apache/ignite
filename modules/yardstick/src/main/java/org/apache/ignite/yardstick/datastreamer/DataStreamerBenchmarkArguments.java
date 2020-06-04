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

package org.apache.ignite.yardstick.datastreamer;

import java.util.Map;
import com.beust.jcommander.Parameter;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.yardstick.IgniteBenchmarkArguments;

/**
 * Represents command line arguments that are specific to streamer benchmarks.
 *
 * @see IgniteBenchmarkArguments
 */
@SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
public class DataStreamerBenchmarkArguments {
    /** */
    @Parameter(names = {"-stcp", "--streamerCachesPrefix"}, description = "Cache name prefix for streamer benchmark")
    private String cachesPrefix = "streamer";

    /** */
    @Parameter(names = {"-stci", "--streamerCachesIndex"}, description = "First cache index for streamer benchmark")
    private int cacheIndex;

    /** */
    @Parameter(names = {"-stcc", "--streamerConcCaches"}, description = "Number of concurrently loaded caches for streamer benchmark")
    private int concurrentCaches = 1;

    /** */
    @Parameter(names = {"--streamerThreadsPerCache"}, description = "Number of concurrent threads per each cache " +
            "for streamer benchmark")
    private int threadsPerCache = 1;

    /** */
    @Parameter(names = {"-stbs", "--streamerBufSize"}, description = "Data streamer buffer size")
    private int bufSize = IgniteDataStreamer.DFLT_PER_NODE_BUFFER_SIZE;

    /** */
    @Parameter(names = {"--streamerBatchSize"},
            description = "Collect entries before passing to java streamer api." +
                    "If set to 1 (default value), than entries will be passed directly.")
    private int batchSize = 1;

    /** */
    @Parameter(names = {"--streamerAllowOverwrite"}, description = "AllowOverwrite streamer parameter.")
    private boolean allowOverwrite;

    /**
     * @return Cache name prefix for caches to be used in {@link IgniteStreamerBenchmark}.
     */
    public String cachesPrefix() {
        return cachesPrefix;
    }

    /**
     * @return First cache index for {@link IgniteStreamerBenchmark}.
     */
    public int cacheIndex() {
        return cacheIndex;
    }

    /**
     * @return Number of concurrently loaded caches for {@link IgniteStreamerBenchmark}.
     */
    public int concurrentCaches() {
        return concurrentCaches;
    }

    /**
     * @return Number of concurrent threads for each cache for {@link IgniteStreamerBenchmark}.
     */
    public int threadsPerCache() {
        return threadsPerCache;
    }

    /**
     * @return Streamer buffer size {@link IgniteStreamerBenchmark} (see {@link IgniteDataStreamer#perNodeBufferSize()}.
     */
    public int bufferSize() {
        return bufSize;
    }

    /**
     * How many entries to collect before sending to java streamer API in either way: passing map to {@link
     * IgniteDataStreamer#addData(Map)}. <br/> If set to 1, {@link IgniteDataStreamer#addData(Object, Object)} method
     * will be used.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Bypass corresponding parameter to streamer.
     */
    public boolean allowOverwrite() {
        return allowOverwrite;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(DataStreamerBenchmarkArguments.class, this);
    }
}
