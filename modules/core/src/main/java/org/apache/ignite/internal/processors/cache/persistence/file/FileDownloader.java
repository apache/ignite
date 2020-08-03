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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Part of direct node to node file downloading
 */
public class FileDownloader {
    /** */
    private final IgniteLogger log;

    /** */
    private static final int CHUNK_SIZE = 16 * 1024 * 1024;

    /** */
    private final Path path;

    /** */
    private long bytesReceived;

    /** */
    private boolean doneTransfer;

    /** */
    private long bytesSent = -1;

    /** */
    private ServerSocketChannel srvChan;

    /** */
    private SocketChannel readChan;

    /** */
    private final GridFutureAdapter<Void> finishFut = new GridFutureAdapter<>();

    /**
     *
     */
    public FileDownloader(IgniteLogger log, Path path) {
        this.log = log;
        this.path = path;
    }

    /**
     * @return Download finish future.
     */
    public IgniteInternalFuture<Void> finishFuture() {
        return finishFut;
    }

    /**
     *
     */
    public InetSocketAddress start() throws IgniteCheckedException {
        try {
            ServerSocketChannel ch = ServerSocketChannel.open();

            ch.bind(null);

            srvChan = ch;

            return (InetSocketAddress)ch.getLocalAddress();
        }
        catch (Exception ex) {
            throw new IgniteCheckedException(ex);
        }
    }

    /**
     *
     */
    public void download() {
        FileChannel writeChan = null;
        SocketChannel readChan = null;

        try {
            File f = new File(path.toUri().getPath());

            if (f.exists())
                f.delete();

            File cacheWorkDir = f.getParentFile();

            if (!cacheWorkDir.exists())
                cacheWorkDir.mkdir();

            readChan = srvChan.accept();

            if (log != null && log.isInfoEnabled())
                log.info("Accepted incoming connection, closing server socket: " + srvChan.getLocalAddress());

            U.closeQuiet(srvChan);

            synchronized (this) {
                if (finishFut.isDone()) {
                    // Already received a response with error.
                    U.closeQuiet(readChan);

                    return;
                }
                else
                    this.readChan = readChan;
            }

            writeChan = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

            if (log != null && log.isInfoEnabled())
                log.info("Started writing file [path=" + path + ", rmtAddr=" + readChan.getRemoteAddress() + ']');

            long pos = 0;

            boolean finish = false;

            while (!finish && !finishFut.isDone()) {
                long transferred = writeChan.transferFrom(readChan, pos, CHUNK_SIZE);

                pos += transferred;

                finish = onBytesReceived(transferred);
            }
        }
        catch (IOException ex) {
            finishFut.onDone(ex);
        }
        finally {
            try {
                onDoneTransfer();
            }
            finally {
                // Safety.
                U.closeQuiet(srvChan);
                U.close(writeChan, log);
                U.close(readChan, log);
            }
        }
    }

    /**
     *
     */
    public void onResult(long size, Throwable th) {
        synchronized (this) {
            if (th != null) {
                bytesSent = 0;

                finishFut.onDone(th);

                U.closeQuiet(readChan);
            }
            else {
                bytesSent = size;

                checkCompleted();
            }
        }
    }

    /**
     * @param transferred Number of bytes transferred.
     * @return {@code True} if should keep reading.
     */
    private boolean onBytesReceived(long transferred) {
        synchronized (this) {
            bytesReceived += transferred;

            return bytesSent != -1 && bytesSent == bytesReceived;
        }
    }

    /**
     * Called when reading thread stopped transferring bytes for any reason.
     */
    private void onDoneTransfer() {
        synchronized (this) {
            doneTransfer = true;

            checkCompleted();
        }
    }

    /**
     *
     */
    private void checkCompleted() {
        // Compare sizes if done reading from the socket and received response from remote node.
        if (doneTransfer && bytesSent != -1) {
            if (bytesReceived == bytesSent)
                finishFut.onDone();
            else {
                finishFut.onDone(new IgniteException("Failed to transfer file (sent and received sizes mismatch) [" +
                    "bytesReceived=" + bytesReceived + ", bytesSent=" + bytesSent + ", file=" + path + ']'));
            }
        }
    }
}
