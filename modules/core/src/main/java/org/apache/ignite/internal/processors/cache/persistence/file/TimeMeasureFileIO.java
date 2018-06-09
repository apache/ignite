package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.U;

public class TimeMeasureFileIO extends FileIODecorator {

    private static final long READ_TIMEOUT = 100;

    private static final long WRITE_TIMEOUT = 500;

    private static final long FSYNC_TIMEOUT = 5000;

    private final File file;

    /**
     * @param delegate File I/O delegate
     */
    public TimeMeasureFileIO(File file, FileIO delegate) {
        super(delegate);
        this.file = file;
    }

    @Override
    public int read(ByteBuffer destBuf) throws IOException {
        long startTime = U.currentTimeMillis();

        int result = super.read(destBuf);

        long time = U.currentTimeMillis() - startTime;

        if (time > READ_TIMEOUT)
            System.err.println("Too long read: " + time + ". " + file);

        return result;
    }

    @Override
    public int read(ByteBuffer destBuf, long position) throws IOException {
        long startTime = U.currentTimeMillis();

        int result = super.read(destBuf, position);

        long time = U.currentTimeMillis() - startTime;

        if (time > READ_TIMEOUT)
            System.err.println("Too long read: " + time + ". " + file);

        return result;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        long startTime = U.currentTimeMillis();

        int result = super.read(buf, off, len);

        long time = U.currentTimeMillis() - startTime;

        if (time > READ_TIMEOUT)
            System.err.println("Too long read: " + time + ". " + file);

        return result;
    }

    @Override
    public int write(ByteBuffer srcBuf) throws IOException {
        long startTime = U.currentTimeMillis();

        int result = super.write(srcBuf);

        long time = U.currentTimeMillis() - startTime;

        if (time > WRITE_TIMEOUT)
            System.err.println("Too long write: " + time + ". " + file);

        return result;
    }

    @Override
    public int write(ByteBuffer srcBuf, long position) throws IOException {
        long startTime = U.currentTimeMillis();

        int result = super.write(srcBuf, position);

        long time = U.currentTimeMillis() - startTime;

        if (time > WRITE_TIMEOUT)
            System.err.println("Too long write: " + time + ". " + file);

        return result;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        long startTime = U.currentTimeMillis();

        super.write(buf, off, len);

        long time = U.currentTimeMillis() - startTime;

        if (time > WRITE_TIMEOUT)
            System.err.println("Too long write: " + time + ". " + file);
    }

    @Override
    public void force() throws IOException {
        long startTime = U.currentTimeMillis();

        super.force();

        long time = U.currentTimeMillis() - startTime;

        if (time > FSYNC_TIMEOUT)
            System.err.println("Too long fsync: " + time);
    }

    @Override
    public void force(boolean withMetadata) throws IOException {
        long startTime = U.currentTimeMillis();

        super.force(withMetadata);

        long time = U.currentTimeMillis() - startTime;

        if (time > FSYNC_TIMEOUT)
            System.err.println("Too long fsync: " + time + ". " + file);
    }
}
