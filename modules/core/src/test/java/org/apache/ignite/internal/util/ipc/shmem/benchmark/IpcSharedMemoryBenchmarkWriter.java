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

package org.apache.ignite.internal.util.ipc.shmem.benchmark;

import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;
import javax.swing.JOptionPane;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcEndpointFactory;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryNativeLoader;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.logger.java.JavaLogger;

/**
 *
 */
public class IpcSharedMemoryBenchmarkWriter implements IpcSharedMemoryBenchmarkParty {
    /** Large send buffer size. */
    public static final int SRC_BUFFER_SIZE = 512 * 1024 * 1024;

    /** */
    private static volatile boolean done;

    /**
     * @param args Args.
     * @throws IgniteCheckedException If failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        IpcSharedMemoryNativeLoader.load(null);

        int nThreads = args.length > 0 ? Integer.parseInt(args[0]) : 1;

        final AtomicLong transferCntr = new AtomicLong();

        Thread collector = new Thread(new Runnable() {
            @SuppressWarnings("BusyWait")
            @Override public void run() {
                try {
                    while (!done) {
                        Thread.sleep(5000);

                        X.println("Transfer rate: " + transferCntr.getAndSet(0) / (1024 * 1024 * 5) + " MB/sec");
                    }
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

            }
        });

        collector.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                System.out.println("Shutting down...");

                done = true;
            }
        });

        for (int i = 0; i < nThreads; i++) {
            final int threadNum = i;

            new Thread(new Runnable() {
                @Override public void run() {
                    IpcEndpoint client = null;

                    try {
                        client = IpcEndpointFactory.connectEndpoint("shmem:" +
                                IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, new JavaLogger());

                        OutputStream space = client.outputStream();

                        byte[] buf = new byte[SRC_BUFFER_SIZE];

                        int pos = 0;

                        while (!done) {
                            int snd = Math.min(DFLT_BUF_SIZE, buf.length - pos);

                            space.write(buf, pos, snd);

                            // Measure only 1 client.
                            if (threadNum == 0)
                                transferCntr.addAndGet(snd);

                            pos += snd;

                            if (pos >= buf.length)
                                pos = 0;
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        if (client != null)
                            client.close();
                    }
                }
            }).start();
        }

        JOptionPane.showMessageDialog(null, "Press OK to stop WRITER.");

        done = true;
    }
}