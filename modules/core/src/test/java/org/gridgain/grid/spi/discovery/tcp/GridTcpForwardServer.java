/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

/**
 * Class implements forwarding between two addresses.
 */
public final class GridTcpForwardServer implements AutoCloseable {
    /** */
    private final Thread mainThread;

    /** */
    private CountDownLatch latch;

    /**
     * @param fromAddr Source address.
     * @param fromPort Source port.
     * @param toAddr Destination address.
     * @param toPort Destination port.
     */
    public GridTcpForwardServer(
        final InetAddress fromAddr,
        final int fromPort,
        final InetAddress toAddr,
        final int toPort
    ) {
        mainThread = new Thread(new Runnable() {
            @Override public void run() {
                ServerSocket inputSock = null;

                Socket outputCon = null;

                try {
                    inputSock = new ServerSocket(fromPort, 0, fromAddr);

                    boolean closed = false;

                    while (!closed) {
                        Socket inputCon = null;

                        try {
                            inputCon = inputSock.accept();

                            outputCon = new Socket(toAddr, toPort);

                            latch = new CountDownLatch(2);

                            new ForwardThread("[" + fromAddr + ":" + fromPort + "->" + toAddr + ":" + toPort + "]",
                                inputCon.getInputStream(), outputCon.getOutputStream()).start();

                            new ForwardThread("[" + toAddr + ":" + toPort + "->" + fromAddr + ":" + fromPort + "]",
                                outputCon.getInputStream(), inputCon.getOutputStream()).start();

                            latch.await();
                        }
                        catch (IOException ignore) {
                            // Ignore and try to reconnect if IOException,
                        }
                        catch (Throwable ignored) {
                            closed = true;
                        }
                        finally {
                            U.closeQuiet(outputCon);
                            U.closeQuiet(inputCon);
                        }
                    }
                }
                catch (IOException ignore) {
                    X.error("Failed to bind to input socket [fromAddr=" + fromAddr + ", fromPort=" + fromPort + "]");
                }
                finally {
                    U.closeQuiet(inputSock);
                    U.closeQuiet(outputCon);
                }
            }
        });

        mainThread.start();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (!mainThread.isInterrupted())
            mainThread.interrupt();
    }

    /**
     * Thread reads data from input stream and write to output stream.
     */
    private class ForwardThread extends Thread {
        /** */
        private final InputStream inputStream;

        /** */
        private final OutputStream outputStream;

        /**
         * @param name Thread name.
         * @param inputStream Input stream.
         * @param outputStream Output stream.
         */
        private ForwardThread(String name, InputStream inputStream, OutputStream outputStream) {
            super(ForwardThread.class.getName() + " " + name);

            this.inputStream = inputStream;
            this.outputStream = outputStream;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                byte[] buf = new byte[1 << 17];

                while (true) {
                    int bytesRead = inputStream.read(buf);

                    if (bytesRead == -1)
                        break;

                    outputStream.write(buf, 0, bytesRead);
                    outputStream.flush();
                }
            }
            catch (IOException e) {
                X.error("IOException while forwarding data [threadName=" + getName() + "]", e);
            }
            finally {
                latch.countDown();
            }
        }
    }
}
