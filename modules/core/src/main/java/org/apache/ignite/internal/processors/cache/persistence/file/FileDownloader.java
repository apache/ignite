package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;

public class FileDownloader {
    private static final int CHUNK_SIZE = 1024 * 1024;

    private final Path path;

    private final GridFutureAdapter<?> fut;

    private final AtomicLong size = new AtomicLong(-1);

    private ServerSocketChannel serverChannel;

    public FileDownloader(Path path, GridFutureAdapter<?> fut) {
        this.path = path;
        this.fut = fut;
    }

    public InetSocketAddress start() throws IgniteCheckedException {
        try {
            final ServerSocketChannel ch = ServerSocketChannel.open();

            fut.listen(new IgniteInClosureX<IgniteInternalFuture<?>>() {
                @Override public void applyx(IgniteInternalFuture<?> future) throws IgniteCheckedException {
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

            return (InetSocketAddress)ch.getLocalAddress();
        }
        catch (Exception ex) {
            throw new IgniteCheckedException(ex);
        }
    }

    public void await(){
        FileChannel writeChannel = null;
        SocketChannel readChannel = null;

        try {
            File f = new File(path.toUri().getPath());

            if (f.exists())
                f.delete();

            writeChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

            readChannel = serverChannel.accept();

            long pos = 0;

            long size = this.size.get();

            while (size == -1 || pos < size) {
                pos += writeChannel.transferFrom(readChannel, pos, CHUNK_SIZE);

                if (size == -1)
                    size = this.size.get();
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

    public void download(long size) {
        if (!this.size.compareAndSet(-1, size))
            fut.onDone(new IgniteException("Size mismatch: " + this.size.get() + " != " + size));
        else
            fut.onDone();
    }
}
