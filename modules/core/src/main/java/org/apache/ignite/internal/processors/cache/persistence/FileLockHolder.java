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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Paths;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Abstract file lock holder.
 * Implementations should provide {@link #lockInfo()} that will appear in error message for concurrent processes
 * that will try to lock the same file and {@link #warningMessage(String)} to print on each lock try.
 *
 * @see GridCacheDatabaseSharedManager.NodeFileLockHolder
 */
public abstract class FileLockHolder implements AutoCloseable {
    /** Lock file name. */
    private static final String lockFileName = "lock";

    /** File. */
    private final File file;

    /** Channel. */
    private final RandomAccessFile lockFile;

    /** Lock. */
    private volatile FileLock lock;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param rootDir Root directory for lock file.
     * @param log Log.
     */
    protected FileLockHolder(String rootDir, IgniteLogger log) {
        try {
            file = Paths.get(rootDir, lockFileName).toFile();

            lockFile = new RandomAccessFile(file, "rw");

            this.log = log;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * This info will appear in error message of concurrent processes that will try to lock on the same file.
     *
     * @return Lock info to store in the file.
     */
    public abstract String lockInfo();

    /**
     * @param lockInfo Existing lock info.
     * @return Warning message.
     */
    protected abstract String warningMessage(String lockInfo);

    /**
     * @param lockWaitTimeMillis During which time thread will try capture file lock.
     * @throws IgniteCheckedException If failed to capture file lock.
     */
    public void tryLock(long lockWaitTimeMillis) throws IgniteCheckedException {
        assert lockFile != null;

        FileChannel ch = lockFile.getChannel();

        String failMsg;

        try {
            String content = null;

            // Try to get lock, if not available wait 1 sec and re-try.
            for (int i = 0; i < lockWaitTimeMillis; i += 1000) {
                try {
                    lock = ch.tryLock(0, 1, false);

                    if (lock != null && lock.isValid()) {
                        writeContent(lockInfo());

                        return;
                    }
                }
                catch (OverlappingFileLockException ignore) {
                    if (content == null)
                        content = readContent();

                    log.warning(warningMessage(content));
                }

                U.sleep(1000);
            }

            if (content == null)
                content = readContent();

            failMsg = "Failed to acquire file lock [holder=" + content + ", time=" + (lockWaitTimeMillis / 1000) +
                " sec, path=" + file.getAbsolutePath() + ']';
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        if (failMsg != null)
            throw new IgniteCheckedException(failMsg);
    }

    /**
     * Write node id (who captured lock) into lock file.
     *
     * @param content Node id.
     * @throws IOException if some fail while write node it.
     */
    private void writeContent(String content) throws IOException {
        FileChannel ch = lockFile.getChannel();

        byte[] bytes = content.getBytes();

        ByteBuffer buf = ByteBuffer.allocate(bytes.length);
        buf.put(bytes);

        buf.flip();

        ch.write(buf, 1);

        ch.force(false);
    }

    /**
     *
     */
    private String readContent() throws IOException {
        FileChannel ch = lockFile.getChannel();

        ByteBuffer buf = ByteBuffer.allocate((int)(ch.size() - 1));

        ch.read(buf, 1);

        String content = new String(buf.array());

        buf.clear();

        return content;
    }

    /**
     * Locked or not.
     */
    public boolean isLocked() {
        return lock != null && lock.isValid();
    }

    /**
     * Releases file lock
     */
    public void release() {
        U.releaseQuiet(lock);
    }

    /**
     * Closes file channel
     */
    @Override public void close() {
        release();

        U.closeQuiet(lockFile);
    }

    /**
     * @return Absolute path to lock file.
     */
    public String lockPath() {
        return file.getAbsolutePath();
    }
}
