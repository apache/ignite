package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.lang.IgniteFutureCancelledException;

public class FileDownloader {
    private static final int CHUNK_SIZE = 1024 * 1024;

    private final Path path;

    private final Executor exec;

    private final GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

    private final AtomicLong size = new AtomicLong(-1);

    private ServerSocketChannel serverChannel;

    public FileDownloader(Path path, Executor exec) {
        this.path = path;
        this.exec = exec;
    }

    public InetSocketAddress start() throws IgniteCheckedException {
        try {
            final ServerSocketChannel ch = ServerSocketChannel.open();

            fut.listen(new IgniteInClosureX<IgniteInternalFuture<Void>>() {
                @Override public void applyx(IgniteInternalFuture<Void> future) throws IgniteCheckedException {
                    try {
                        ch.close();
                    }
                    catch (Exception ex) {
                        throw new IgniteCheckedException(ex);
                    }
                }
            });

            ch.bind(null);

            serverChannel = ch;

            exec.execute(new Worker());

            return (InetSocketAddress) ch.getLocalAddress();
        }
        catch (Exception ex) {
            throw new IgniteCheckedException(ex);
        }
    }

    public IgniteInternalFuture<Void> download(long size) {
        if (!this.size.compareAndSet(-1, size))
            throw new IgniteException("Size mismatch: " + this.size.get() + " != " + size);

        return fut;
    }

    public IgniteInternalFuture<Void> future() {
        return fut;
    }

    public boolean cancel() {
        return fut.onDone(new IgniteFutureCancelledException("Download cancelled"));
    }

    private class Worker implements Runnable {
        @Override public void run() {
            FileChannel writeChannel = null;
            SocketChannel readChannel = null;

            try {
                File f = new File(path.toUri().getPath());

                if (f.exists())
                    f.delete();

                writeChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

                readChannel = serverChannel.accept();

                long pos = 0;

                long size = FileDownloader.this.size.get();

                while (size == -1 || pos < size) {
                    pos += writeChannel.transferFrom(readChannel, pos, CHUNK_SIZE);

                    if (size == -1)
                        size = FileDownloader.this.size.get();
                }

                fut.onDone();
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
                    throw new IgniteException("Could not close file: " + path);
                }

                try {
                    if (readChannel != null)
                        readChannel.close();
                }
                catch (IOException ex) {
                    throw new IgniteException("Could not close socket");
                }
            }
        }
    }

}
