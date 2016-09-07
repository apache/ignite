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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;

/**
 * Marshaller work directory handlers.
 */
public class MarshallerWorkDirectory {
    /** */
    private static final String fileExt = ".classname";

    /** */
    private static final GridStripedLock fileLock = new GridStripedLock(32);

    /**
     * Gets the type name from a file in the marshaller work directory.
     *
     * @param key Key.
     * @param workDir Work dir.
     * @return Type name.
     * @throws IgniteCheckedException When the file is not found.
     */
    public static String getTypeNameFromFile(Object key, File workDir) throws IgniteCheckedException {
        String fileName = key + fileExt;

        Lock lock = fileLock(fileName);

        lock.lock();

        try {
            File file = new File(workDir, fileName);

            try (FileInputStream in = new FileInputStream(file)) {
                FileLock fileLock = fileLock(in.getChannel(), true);

                assert fileLock != null : fileName;

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    return reader.readLine();
                }
            }
            catch (IOException ignored) {
                throw new IgniteCheckedException("Class definition was not found " +
                    "at marshaller cache and local file. " +
                    "[id=" + key + ", file=" + file.getAbsolutePath() + ']');
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Writes the type name to a file in a marshaller work dir.
     * @param key Type key.
     * @param typeName Class name.
     * @param log Logger.
     * @param workDir Work directory.
     */
    public static void writeTypeNameToFile(Object key, String typeName, IgniteLogger log, File workDir) {
        String fileName = key + fileExt;

        Lock lock = fileLock(fileName);

        lock.lock();

        try {
            File file = new File(workDir, fileName);

            try (FileOutputStream out = new FileOutputStream(file)) {
                FileLock fileLock = fileLock(out.getChannel(), false);

                assert fileLock != null : fileName;

                try (Writer writer = new OutputStreamWriter(out)) {
                    writer.write(typeName);

                    writer.flush();
                }
            }
            catch (IOException e) {
                U.error(log, "Failed to write class name to file [id=" + key +
                    ", clsName=" + typeName + ", file=" + file.getAbsolutePath() + ']', e);
            }
            catch(OverlappingFileLockException ignored) {
                if (log.isDebugEnabled())
                    log.debug("File already locked (will ignore): " + file.getAbsolutePath());
            }
            catch (IgniteInterruptedCheckedException e) {
                U.error(log, "Interrupted while waiting for acquiring file lock: " + file, e);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param ch File channel.
     * @param shared Shared.
     */
    private static FileLock fileLock(
        FileChannel ch,
        boolean shared
    ) throws IOException, IgniteInterruptedCheckedException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (true) {
            FileLock fileLock = ch.tryLock(0L, Long.MAX_VALUE, shared);

            if (fileLock == null)
                U.sleep(rnd.nextLong(50));
            else
                return fileLock;
        }
    }

    /**
     * @param fileName File name.
     * @return Lock instance.
     */
    private static Lock fileLock(String fileName) {
        return fileLock.getLock(fileName.hashCode());
    }
}
