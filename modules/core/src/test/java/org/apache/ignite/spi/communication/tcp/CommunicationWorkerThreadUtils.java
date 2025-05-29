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

package org.apache.ignite.spi.communication.tcp;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationWorker;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;

/**
 * Utils to work with communication worker threads.
 */
class CommunicationWorkerThreadUtils {
    /**
     * We need to interrupt communication worker client nodes so that
     * closed connection won't automatically reopen when we don't expect it.
     *
     * @param clientName The name of the client whose threads we want to interrupt.
     * @param log        The logger to use while joining the interrupted threads.
     */
    static void interruptCommWorkerThreads(String clientName, IgniteLogger log) {
        List<Thread> tcpCommWorkerThreads = Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.getName().contains(CommunicationWorker.WORKER_NAME))
            .filter(t -> t.getName().contains(clientName))
            .collect(Collectors.toList());

        for (Thread tcpCommWorkerThread : tcpCommWorkerThreads) {
            U.interrupt(tcpCommWorkerThread);

            U.join(tcpCommWorkerThread, log);
        }
    }

    /**
     * Notifies the {@link TcpCommunicationSpi} components about a node leaving the cluster.
     *
     * @param consistentId Consistent id of the node.
     * @param nodeId Left node ID.
     */
    static void onNodeLeft(TcpCommunicationSpi spi, Object consistentId, UUID nodeId) {
        assert nodeId != null;

        ((TcpCommunicationMetricsListener)U.field(spi, "metricsLsnr")).onNodeLeft(consistentId);
        ((ConnectionClientPool)U.field(spi, "clientPool")).onNodeLeft(nodeId);
    }
}
