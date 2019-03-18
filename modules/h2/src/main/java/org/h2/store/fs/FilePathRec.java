/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;

/**
 * A file system that records all write operations and can re-play them.
 */
public class FilePathRec extends FilePathWrapper {

    private static final FilePathRec INSTANCE = new FilePathRec();

    private static Recorder recorder;

    private boolean trace;

    /**
     * Register the file system.
     */
    public static void register() {
        FilePath.register(INSTANCE);
    }

    /**
     * Set the recorder class.
     *
     * @param recorder the recorder
     */
    public static void setRecorder(Recorder recorder) {
        FilePathRec.recorder = recorder;
    }

    @Override
    public boolean createFile() {
        log(Recorder.CREATE_NEW_FILE, name);
        return super.createFile();
    }

    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit,
            boolean inTempDir) throws IOException {
        log(Recorder.CREATE_TEMP_FILE, unwrap(name) + ":" + suffix + ":" +
                deleteOnExit + ":" + inTempDir);
        return super.createTempFile(suffix, deleteOnExit, inTempDir);
    }

    @Override
    public void delete() {
        log(Recorder.DELETE, name);
        super.delete();
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileRec(this, super.open(mode), name);
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        log(Recorder.OPEN_OUTPUT_STREAM, name);
        return super.newOutputStream(append);
    }

    @Override
    public void moveTo(FilePath newPath, boolean atomicReplace) {
        log(Recorder.RENAME, unwrap(name) + ":" + unwrap(newPath.name));
        super.moveTo(newPath, atomicReplace);
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    /**
     * Log the operation.
     *
     * @param op the operation
     * @param fileName the file name(s)
     */
    void log(int op, String fileName) {
        log(op, fileName, null, 0);
    }

    /**
     * Log the operation.
     *
     * @param op the operation
     * @param fileName the file name
     * @param data the data or null
     * @param x the value or 0
     */
    void log(int op, String fileName, byte[] data, long x) {
        if (recorder != null) {
            recorder.log(op, fileName, data, x);
        }
    }

    /**
     * Get the prefix for this file system.
     *
     * @return the prefix
     */
    @Override
    public String getScheme() {
        return "rec";
    }

}

/**
 * A file object that records all write operations and can re-play them.
 */
class FileRec extends FileBase {

    private final FilePathRec rec;
    private final FileChannel channel;
    private final String name;

    FileRec(FilePathRec rec, FileChannel file, String fileName) {
        this.rec = rec;
        this.channel = file;
        this.name = fileName;
    }

    @Override
    public void implCloseChannel() throws IOException {
        channel.close();
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position);
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        channel.position(pos);
        return this;
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        rec.log(Recorder.TRUNCATE, name, null, newLength);
        channel.truncate(newLength);
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        byte[] buff = src.array();
        int len = src.remaining();
        if (src.position() != 0 || len != buff.length) {
            int offset = src.arrayOffset() + src.position();
            buff = Arrays.copyOfRange(buff, offset, offset + len);
        }
        int result = channel.write(src);
        rec.log(Recorder.WRITE, name, buff, channel.position());
        return result;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        byte[] buff = src.array();
        int len = src.remaining();
        if (src.position() != 0 || len != buff.length) {
            int offset = src.arrayOffset() + src.position();
            buff = Arrays.copyOfRange(buff, offset, offset + len);
        }
        int result = channel.write(src, position);
        rec.log(Recorder.WRITE, name, buff, position);
        return result;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return name;
    }

}
