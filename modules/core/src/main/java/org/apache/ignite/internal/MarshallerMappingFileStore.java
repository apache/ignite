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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContext;

/**
 * File-based persistence provider for {@link MarshallerContextImpl}.
 *
 * Saves mappings in format <b>{typeId}.classname{platformId}</b>, e.g. 123.classname0.
 *
 * It writes new mapping when it is accepted by all grid members and reads mapping
 * when a classname is requested but is not presented in local cache of {@link MarshallerContextImpl}.
 */
final class MarshallerMappingFileStore {
    /** */
    private static final GridStripedLock fileLock = new GridStripedLock(32);

    /** */
    private final IgniteLogger log;

    /** Marshaller mapping directory */
    private final File workDir;

    /** */
    private final String FILE_EXTENSION = ".classname";

    /**
     * @param igniteWorkDir Ignite work directory
     * @param log Logger.
     */
    MarshallerMappingFileStore(String igniteWorkDir, IgniteLogger log) throws IgniteCheckedException {
        workDir = U.resolveWorkDirectory(igniteWorkDir, "marshaller", false);
        this.log = log;
    }

    /**
     * Creates marshaller mapping file store with custom predefined work directory
     * @param log logger.
     * @param marshallerMappingFileStoreDir custom marshaller work directory
     */
    MarshallerMappingFileStore(final IgniteLogger log, final File marshallerMappingFileStoreDir) {
        this.workDir = marshallerMappingFileStoreDir;
        this.log = log;
    }

    /**
     * @param platformId Platform id.
     * @param typeId Type id.
     * @param typeName Type name.
     */
    void writeMapping(byte platformId, int typeId, String typeName) {
        String fileName = getFileName(platformId, typeId);

        Lock lock = fileLock(fileName);

        lock.lock();

        try {
            File file = new File(workDir, fileName);

            try (FileOutputStream out = new FileOutputStream(file)) {
                FileLock fileLock = fileLock(out.getChannel(), false);

                assert fileLock != null : fileName;

                try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    writer.write(typeName);

                    writer.flush();
                }
            }
            catch (IOException e) {
                U.error(log, "Failed to write class name to file [platformId=" + platformId + "id=" + typeId +
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
     * @param platformId Platform id.
     * @param typeId Type id.
     */
    String readMapping(byte platformId, int typeId) throws IgniteCheckedException {
        String fileName = getFileName(platformId, typeId);

        Lock lock = fileLock(fileName);

        lock.lock();

        try {
            File file = new File(workDir, fileName);

            try (FileInputStream in = new FileInputStream(file)) {
                FileLock fileLock = fileLock(in.getChannel(), true);

                assert fileLock != null : fileName;

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    return reader.readLine();
                }
            }
            catch (IOException ignored) {
                return null;
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Restores all mappings available in file system to marshaller context.
     * This method should be used only on node startup.
     *
     * @param marshCtx Marshaller context to register mappings.
     */
    void restoreMappings(MarshallerContext marshCtx) throws IgniteCheckedException {
        for (File file : workDir.listFiles()) {
            String name = file.getName();

            byte platformId = getPlatformId(name);

            int typeId = getTypeId(name);

            try (FileInputStream in = new FileInputStream(file)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String className = reader.readLine();

                    marshCtx.registerClassNameLocally(platformId, typeId, className);
                }
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Reading marshaller mapping from file "
                    + name
                    + " failed."
                    , e);
            }
        }
    }

    /**
     * @param fileName Name of file with marshaller mapping information.
     * @throws IgniteCheckedException If file name format is broken.
     */
    private byte getPlatformId(String fileName) throws IgniteCheckedException {
        String lastSymbol = fileName.substring(fileName.length() - 1);

        byte platformId;

        try {
            platformId = Byte.parseByte(lastSymbol);
        }
        catch (NumberFormatException e) {
            throw new IgniteCheckedException("Reading marshaller mapping from file "
                + fileName
                + " failed; last symbol of file name is expected to be numeric.", e);
        }

        return platformId;
    }

    /**
     * @param fileName Name of file with marshaller mapping information.
     * @throws IgniteCheckedException If file name format is broken.
     */
    private int getTypeId(String fileName) throws IgniteCheckedException {
        int typeId;

        try {
            typeId = Integer.parseInt(fileName.substring(0, fileName.indexOf(FILE_EXTENSION)));
        }
        catch (NumberFormatException e) {
            throw new IgniteCheckedException("Reading marshaller mapping from file "
                + fileName
                + " failed; type ID is expected to be numeric.", e);
        }

        return typeId;
    }

    /**
     * @param platformId Platform id.
     * @param typeId Type id.
     */
    private String getFileName(byte platformId, int typeId) {
        return typeId + FILE_EXTENSION + platformId;
    }

    /**
     * @param fileName File name.
     * @return Lock instance.
     */
    private static Lock fileLock(String fileName) {
        return fileLock.getLock(fileName.hashCode());
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
}
