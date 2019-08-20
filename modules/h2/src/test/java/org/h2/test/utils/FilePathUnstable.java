/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import java.util.Random;

import org.h2.store.fs.FileBase;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;

/**
 * An unstable file system. It is used to simulate file system problems (for
 * example out of disk space).
 */
public class FilePathUnstable extends FilePathWrapper {

    private static final FilePathUnstable INSTANCE = new FilePathUnstable();

    private static final IOException DISK_FULL = new IOException("Disk full");

    private static int diskFullOffCount;

    private static boolean partialWrites;

    private static Random random = new Random(1);

    /**
     * Register the file system.
     *
     * @return the instance
     */
    public static FilePathUnstable register() {
        FilePath.register(INSTANCE);
        return INSTANCE;
    }

    /**
     * Set the number of write operations before the disk is full, and the
     * random seed (for partial writes).
     *
     * @param count the number of write operations (0 to never fail,
     *            Integer.MAX_VALUE to count the operations)
     * @param seed the new seed
     */
    public void setDiskFullCount(int count, int seed) {
        diskFullOffCount = count;
        random.setSeed(seed);
    }

    public int getDiskFullCount() {
        return diskFullOffCount;
    }

    /**
     * Whether partial writes are possible (writing only part of the data).
     *
     * @param partialWrites true to enable
     */
    public void setPartialWrites(boolean partialWrites) {
        FilePathUnstable.partialWrites = partialWrites;
    }

    boolean getPartialWrites() {
        return partialWrites;
    }

    /**
     * Get a buffer with a subset (the head) of the data of the source buffer.
     *
     * @param src the source buffer
     * @return a buffer with a subset of the data
     */
    ByteBuffer getRandomSubset(ByteBuffer src) {
        int len = src.remaining();
        len = Math.min(4096, Math.min(len, 1 + random.nextInt(len)));
        ByteBuffer temp = ByteBuffer.allocate(len);
        src.get(temp.array());
        return temp;
    }

    /**
     * Check if the simulated problem occurred.
     * This call will decrement the countdown.
     *
     * @throws IOException if the simulated power failure occurred
     */
    void checkError() throws IOException {
        if (diskFullOffCount == 0) {
            return;
        }
        if (--diskFullOffCount > 0) {
            return;
        }
        if (diskFullOffCount >= -1) {
            diskFullOffCount--;
            throw DISK_FULL;
        }
    }

    @Override
    public void createDirectory() {
        super.createDirectory();
    }

    @Override
    public boolean createFile() {
        return super.createFile();
    }

    @Override
    public void delete() {
        super.delete();
    }

    @Override
    public boolean exists() {
        return super.exists();
    }

    @Override
    public String getName() {
        return super.getName();
    }

    @Override
    public long lastModified() {
        return super.lastModified();
    }

    @Override
    public FilePath getParent() {
        return super.getParent();
    }

    @Override
    public boolean isAbsolute() {
        return super.isAbsolute();
    }

    @Override
    public boolean isDirectory() {
        return super.isDirectory();
    }

    @Override
    public boolean canWrite() {
        return super.canWrite();
    }

    @Override
    public boolean setReadOnly() {
        return super.setReadOnly();
    }

    @Override
    public long size() {
        return super.size();
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        return super.newDirectoryStream();
    }

    @Override
    public FilePath toRealPath() {
        return super.toRealPath();
    }

    @Override
    public InputStream newInputStream() throws IOException {
        return super.newInputStream();
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileUnstable(this, super.open(mode));
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        return super.newOutputStream(append);
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        super.moveTo(newName, atomicReplace);
    }

    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit,
            boolean inTempDir) throws IOException {
        return super.createTempFile(suffix, deleteOnExit, inTempDir);
    }

    @Override
    public String getScheme() {
        return "unstable";
    }

}

/**
 * An file that checks for errors before each write operation.
 */
class FileUnstable extends FileBase {

    private final FilePathUnstable file;
    private final FileChannel channel;
    private boolean closed;

    FileUnstable(FilePathUnstable file, FileChannel channel) {
        this.file = file;
        this.channel = channel;
    }

    @Override
    public void implCloseChannel() throws IOException {
        channel.close();
        closed = true;
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
    public int read(ByteBuffer dst, long pos) throws IOException {
        return channel.read(dst, pos);
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        channel.position(pos);
        return this;
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        checkError();
        channel.truncate(newLength);
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        checkError();
        channel.force(metaData);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        checkError();
        if (file.getPartialWrites()) {
            return channel.write(file.getRandomSubset(src));
        }
        return channel.write(src);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        checkError();
        if (file.getPartialWrites()) {
            return channel.write(file.getRandomSubset(src), position);
        }
        return channel.write(src, position);
    }

    private void checkError() throws IOException {
        if (closed) {
            throw new IOException("Closed");
        }
        file.checkError();
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return "unstable:" + file.toString();
    }

}