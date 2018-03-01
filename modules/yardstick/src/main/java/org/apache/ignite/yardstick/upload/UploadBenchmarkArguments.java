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

import com.beust.jcommander.Parameter;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.ignite.IgniteDataStreamer;

public class UploadBenchmarkArguments {
    /** Whither or not temporary disable Write Ahead Log during upload. */
    @Parameter(names = {"--disable-wal"},
        arity = 1,
        description = "Upload benchmark only: " +
            "turn off Write Ahead Log before data uploading " +
            "and turn it on again when upload is done.")
    private boolean switchWal = false;

    /**
     * Parameters for jdbc connection, that only uploads data.
     *
     * We can't just pass entire params string, due to yardstick, which relies on bash,
     * has some troubles with escaping ampersand character.
     */
    @Parameter(names = {"--sql-jdbc-params"},
        variableArity = true,
        description = "Upload benchmark only: " +
            "Additional url parameters (space separated key=value) for special jdbc connection that only uploads data. ")
    private List<String> uploadJdbcParams = Collections.emptyList();

    @Parameter(names = {"--sql-copy-packet-size"},
        description = "Upload benchmark only: use custom packet_size (in bytes) for copy command.")
    private Long copyPacketSize = null;

    @Parameter(names = {"--streamer-node-buf-size"},
        description = "Native Streamer benchmark only: Set streamer's perNodeBufferSize property")
    private Integer streamerBufSize = null;

    @Parameter(names = {"--streamer-node-par-ops"},
        description = "Native Streamer benchmark only: Set streamer's perNodeParallelOperations property")
    private Integer streamerNodeParOps = null;

    /**
     * @return Switch wal.
     */
    public boolean switchWal() {
        return switchWal;
    }

    /** @return parameters for jdbc url */
    public List<String> uploadJdbcParams() {
        return uploadJdbcParams;
    }

    /** @return packet_size value for copy command or {@code null} for default value */
    @Nullable
    public Long copyPacketSize() {
        return copyPacketSize;
    }

    /**
     * @return Value for {@link IgniteDataStreamer#perNodeBufferSize(int)}.
     */
    @Nullable
    public Integer streamerBufSize() {
        return streamerBufSize;
    }

    /**
     * @return Value for {@link IgniteDataStreamer#perNodeParallelOperations(int)}
     */
    @Nullable
    public Integer streamerNodeParOps() {
        return streamerNodeParOps;
    }
}
