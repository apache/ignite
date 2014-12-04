/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;

/**
 * Class implements forwarding between two addresses.
 */
public final class GridTcpForwarder implements AutoCloseable {
    /** */
    private final IgniteLogger log;

    /** */
    private final Thread mainThread;

    /** */
    private final ServerSocket inputSock;

    /**
     * @param fromAddr Source address.
     * @param fromPort Source port.
     * @param toAddr Destination address.
     * @param toPort Destination port.
     * @param log Logger.
     * @throws IOException if an I/O error occurs when opening the socket.
     */
    public GridTcpForwarder(
        final InetAddress fromAddr,
        final int fromPort,
        final InetAddress toAddr,
        final int toPort,
        final IgniteLogger log
    ) throws IOException {
        inputSock = new ServerSocket(fromPort, 0, fromAddr);

        this.log = log;

        mainThread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    boolean closed = false;

                    while (!closed) {
                        Socket inputCon = null;
                        Socket outputCon = null;

                        Thread forwardThread1 = null;
                        Thread forwardThread2 = null;

                        try {
                            inputCon = inputSock.accept();

                            outputCon = new Socket(toAddr, toPort);

                            forwardThread1 = new ForwardThread(
                                "ForwardThread [" + fromAddr + ":" + fromPort + "->" + toAddr + ":" + toPort + "]",
                                inputCon.getInputStream(), outputCon.getOutputStream()
                            );

                            forwardThread2 = new ForwardThread(
                                "ForwardThread [" + toAddr + ":" + toPort + "->" + fromAddr + ":" + fromPort + "]",
                                outputCon.getInputStream(), inputCon.getOutputStream()
                            );

                            forwardThread1.start();
                            forwardThread2.start();

                            U.join(forwardThread1, log);
                            U.join(forwardThread2, log);
                        }
                        catch (IOException ignore) {
                            if (inputSock.isClosed())
                                closed = true;
                        }
                        catch (Throwable ignored) {
                            closed = true;
                        }
                        finally {
                            U.closeQuiet(outputCon);
                            U.closeQuiet(inputCon);

                            U.interrupt(forwardThread1);
                            U.interrupt(forwardThread2);

                            U.join(forwardThread1, log);
                            U.join(forwardThread2, log);
                        }
                    }
                }
                finally {
                    U.closeQuiet(inputSock);
                }
            }
        }, "GridTcpForwarder [" + fromAddr + ":" + fromPort + "->" + toAddr + ":" + toPort + "]");

        mainThread.start();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        U.closeQuiet(inputSock);

        U.interrupt(mainThread);
        U.join(mainThread, log);
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
            super(name);

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
                log.error("IOException while forwarding data [threadName=" + getName() + "]", e);
            }
        }
    }
}
