/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem.benchmark;

import org.gridgain.grid.kernal.ggfs.common.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.ipc.shmem.*;
import org.gridgain.testframework.junits.*;

import java.io.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridGgfsSharedMemoryBenchmarkReader implements GridIpcSharedMemoryBenchmarkParty {
    /** */
    private static volatile boolean done;

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws IOException {
        int nThreads = (args.length > 1 ? Integer.parseInt(args[1]) : 1);

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
            new Thread(new Runnable() {
                @Override public void run() {
                    InputStream in = null;

                    try {
                        GridIpcSharedMemoryServerEndpoint srv = new GridIpcSharedMemoryServerEndpoint();

                        new GridTestResources().inject(srv);

                        srv.start();

                        GridIpcEndpoint client = srv.accept();

                        in = client.inputStream();

                        DataInput din = new GridGgfsDataInputStream(in);

                        byte[] buf = new byte[DFLT_BUF_SIZE];

                        byte[] hdr = new byte[GridGgfsMarshaller.HEADER_SIZE];

                        while (!done) {
                            din.readFully(hdr);

                            long reqId = U.bytesToLong(hdr, 0);

                            GridGgfsIpcCommand cmd = GridGgfsIpcCommand.valueOf(U.bytesToInt(hdr, 8));

                            long uuid = U.bytesToLong(hdr, 12);

                            int writeLen = U.bytesToInt(hdr, 20);

                            int read = 0;

                            while (read < writeLen) {
                                int len = Math.min(buf.length, writeLen - read);

                                din.readFully(buf, 0, len);

                                read += len;
                            }

                            transferCntr.addAndGet(writeLen);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        try {
                            in.close();
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }

        System.in.read();

        done = true;
    }
}
