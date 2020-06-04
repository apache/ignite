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

package org.apache.ignite.yardstick;

import com.beust.jcommander.Parameter;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;

/**
 * Input arguments for Ignite benchmarks.
 */
@SuppressWarnings("FieldMayBeFinal")
public class IgniteThinBenchmarkArguments {
    /** */
    @Parameter(names = {"--thin-client-pool"}, description = "Type of thin client pool")
    private ClientPoolType clientPoolType = ClientPoolType.THREAD_LOCAL;

    /** */
    @Parameter(names = {"--thin-client-pool-size"}, description = "Size of clients pool (only makes sence if " +
            "ROUND_ROBIN client pool is used)")
    private int clientPoolSize = 10;

    /** */
    @Parameter(names = {"--thin-client-connection-timeout"}, description = "Timeout to wait for server-side client " +
            "connector startup")
    private long connectionTimeout = 10_000L;

    /** */
    @Parameter(names = {"--thin-client-partitions-aware"}, description = "Enable partitions awareness for client")
    private boolean clientPartitionsAware;

    /**
     * @return Client pool type.
     */
    public ClientPoolType clientPoolType() {
        return clientPoolType;
    }

    /**
     * @return Client pool size.
     */
    public int clientPoolSize() {
        return clientPoolSize;
    }

    /**
     * @return Client connection timeout.
     */
    public long connectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @return Client partition awareness.
     */
    public boolean clientPartitionsAware() {
        return clientPartitionsAware;
    }

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return GridToStringBuilder.toString(IgniteThinBenchmarkArguments.class, this);
    }

    /** */
    enum ClientPoolType {
        /** Thread local client pool (one client for each thread). */
        THREAD_LOCAL,

        /** Single client pool (only one client is used for all theads). */
        SINGLE_CLIENT,

        /** Round robin pool (on every request next client is used from fixed size clients collection). */
        ROUND_ROBIN
    }
}
