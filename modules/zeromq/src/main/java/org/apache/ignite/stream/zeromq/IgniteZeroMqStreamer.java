/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.stream.zeromq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.jetbrains.annotations.NotNull;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

/**
 * This streamer uses https://github.com/zeromq/jeromq/.
 * Implements socket types listed {@link ZeroMqTypeSocket}.
 */
public class IgniteZeroMqStreamer<K, V> extends StreamAdapter<byte[], K, V> implements AutoCloseable {
    /** Logger. */
    protected IgniteLogger log;

    /** Parameter {@code True} if streamer started. */
    private volatile boolean isStarted = false;

    /** Process stream asynchronously */
    private ExecutorService zeroMqExSrv;

    /** ZeroMQ context. */
    private ZMQ.Context ctx;

    /** ZeroMQ context threads. */
    private int ioThreads;

    /** ZeroMQ socket type. */
    private int socketType;

    /** ZeroMQ socket address. */
    private String addr;

    /** ZeroMQ topic name. */
    private byte[] topic;

    /** Maximum time to wait. */
    private long timeout = 5_000;

    /**
     * @param ioThreads Threads on context.
     * @param socketType Socket type.
     * @param addr Address to connect zmq.
     * @param topic Topic name for PUB-SUB socket type, otherwise null.
     */
    public IgniteZeroMqStreamer(int ioThreads, ZeroMqTypeSocket socketType, @NotNull String addr, byte[] topic) {
        A.ensure(ioThreads > 0, "ioThreads has to larger than 0.");
        A.ensure(!"".equals(addr), "addr cannot be empty.");
        A.ensure(socketType != null, "socketType has to be specified.");

        this.ioThreads = ioThreads;
        this.addr = addr;
        this.topic = topic;
        this.socketType = socketType.getType();
    }

    /**
     * Starts ZeroMQ streamer.
     */
    public void start() {
        A.ensure(getSingleTupleExtractor() != null || getMultipleTupleExtractor() != null, "ZeroMq extractor.");

        log = getIgnite().log();

        if (isStarted) {
            log.warning("Attempted to start an already started ZeroMQ streamer");
            return;
        }

        isStarted = true;

        zeroMqExSrv = Executors.newSingleThreadExecutor();

        ctx = ZMQ.context(ioThreads);

        zeroMqExSrv.execute(() -> {
            ZMQ.Socket socket = ctx.socket(socketType);
            socket.connect(addr);

            if (ZeroMqTypeSocket.SUB.getType() == socketType)
                socket.subscribe(topic);

            while (isStarted) {
                try {
                    byte[] msg = socket.recv(0);

                    if (ZeroMqTypeSocket.SUB.getType() == socketType) {
                        if (socket.hasReceiveMore()) {
                            addMessage(socket.recv(0));
                            continue;
                        }
                    }

                    addMessage(msg);
                }
                catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                        break;
                    }
                }
            }

            socket.close();
        });
    }

    /**
     * Stops ZeroMQ streamer.
     */
    @Override public void close() throws Exception {
        isStarted = false;

        if (ctx != null)
            ctx.close();

        if (zeroMqExSrv != null) {
            zeroMqExSrv.shutdown();

            try {
                if (!zeroMqExSrv.awaitTermination(timeout, TimeUnit.MILLISECONDS))
                    log.warning("Timed out waiting for consumer threads to shut down, exiting uncleanly.");
            }
            catch (InterruptedException ignored) {
                zeroMqExSrv.shutdownNow();
                log.error("Interrupted during shutdown, exiting uncleanly.");
            }
        }
    }
}
