package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

public class FileUploader {
    private static final int CHUNK_SIZE = 1024 * 1024;

    private final Path path;

    private final InetSocketAddress address;

    private final Executor exec;

    private final GridFutureAdapter<Long> fut = new GridFutureAdapter<>();

    public FileUploader(Path path, InetSocketAddress address, Executor exec) {
        this.path = path;
        this.address = address;
        this.exec = exec;
    }

    public IgniteInternalFuture<Long> upload() {
        exec.execute(new Worker());

        return fut;
    }

    private class Worker implements Runnable {
        @Override public void run() {
            FileChannel readChannel = null;
            SocketChannel writeChannel = null;

            try {
                readChannel = FileChannel.open(path, StandardOpenOption.READ);

                writeChannel = SocketChannel.open(address);

                long written = 0;

                long size = readChannel.size();

                while (written < size)
                    written += readChannel.transferTo(written, CHUNK_SIZE, writeChannel);

                fut.onDone(written);
            }
            catch (IOException ex) {
                fut.onDone(ex);
            }
            finally {
                try {
                    if (writeChannel != null)
                        writeChannel.close();
                }
                catch (IOException ex) {
                    throw new IgniteException("Could not close socket: " + address);
                }

                try {
                    if (readChannel != null)
                        readChannel.close();
                }
                catch (IOException ex) {
                    throw new IgniteException("Could not close file: " + path);
                }
            }
        }
    }
}
