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
import org.gridgain.grid.logger.java.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.ipc.shmem.*;

import java.io.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.processors.ggfs.GridGgfsIpcCommand.*;

/**
 *
 */
public class GridGgfsSharedMemoryBenchmarkWriter implements GridIpcSharedMemoryBenchmarkParty {
    /** */
    private static volatile boolean done;

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws IOException {
        int nThreads = args.length > 1 ? Integer.parseInt(args[1]) : 1;

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

        final long id = 1;

        for (int i = 0; i < nThreads; i++) {
            final int threadNum = i;

            new Thread(new Runnable() {
                @Override public void run() {
                    OutputStream os = null;

                    try {
                        GridIpcEndpoint client = GridIpcEndpointFactory.connectEndpoint("shmem:" +
                            GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, new GridJavaLogger());

                        os = new BufferedOutputStream(client.outputStream());

                        ObjectOutput out = new GridGgfsDataOutputStream(os);

                        byte[] buf = new byte[DFLT_BUF_SIZE];

                        long cnt = 0;

                        byte[] hdr = new byte[GridGgfsMarshaller.HEADER_SIZE];

                        GridGgfsStreamControlRequest msg = new GridGgfsStreamControlRequest();

                        long streamId = 1;

                        while (!done) {
                            buf[0] = (byte)cnt;
                            buf[DFLT_BUF_SIZE - 1] = (byte)cnt;

                            cnt++;

                            msg.command(WRITE_BLOCK);
                            msg.streamId(id);
                            msg.data(buf);
                            msg.position(0);
                            msg.length(buf.length);

                            int off = 0;

                            U.longToBytes(-1, hdr, off);
                            off += 8;

                            U.intToBytes(msg.command().ordinal(), hdr, off);
                            off += 4;

                            U.longToBytes(streamId, hdr, off);
                            off += 8;

                            U.intToBytes(buf.length, hdr, off);

                            synchronized (this) {
                                assert msg.command() == GridGgfsIpcCommand.WRITE_BLOCK;

                                out.write(hdr);

                                out.write(buf);
                            }

                            // Measure only 1 client.
                            if (threadNum == 0)
                                transferCntr.addAndGet(buf.length);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        try {
                            if (os != null)
                                os.close();
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
