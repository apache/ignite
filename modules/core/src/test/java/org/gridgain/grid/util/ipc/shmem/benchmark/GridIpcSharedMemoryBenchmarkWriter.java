/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem.benchmark;

import org.apache.ignite.logger.java.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.ipc.shmem.*;

import javax.swing.*;
import java.io.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridIpcSharedMemoryBenchmarkWriter implements GridIpcSharedMemoryBenchmarkParty {
    /** Large send buffer size. */
    public static final int SRC_BUFFER_SIZE = 512 * 1024 * 1024;

    /** */
    private static volatile boolean done;

    /**
     * @param args Args.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        GridIpcSharedMemoryNativeLoader.load();

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
                    GridIpcEndpoint client = null;

                    try {
                        client = GridIpcEndpointFactory.connectEndpoint("shmem:" +
                            GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, new GridJavaLogger());

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
