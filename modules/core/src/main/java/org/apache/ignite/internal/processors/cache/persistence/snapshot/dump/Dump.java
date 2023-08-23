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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.readCacheData;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_METAFILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpCacheFutureTask.DUMP_FILE_NAME;

/**
 *
 */
public class Dump {
    /** Dump directory. */
    private final File dumpDir;

    /** */
    private final GridKernalContext cctx;

    /**
     * @param dumpDir Dump directory.
     */
    public Dump(GridKernalContext cctx, File dumpDir) throws IgniteCheckedException {
        this.cctx = cctx;
        this.dumpDir = dumpDir;

        File binaryMeta = new File(dumpDir, DFLT_BINARY_METADATA_PATH);
        File marshaller = new File(dumpDir, DFLT_MARSHALLER_PATH);

        A.ensure(dumpDir != null, "dump directory is null");
        A.ensure(dumpDir.exists(), "dump directory not exists");
        A.ensure(binaryMeta.exists(), "binary metadata directory not exists");
        A.ensure(marshaller.exists(), "marshaller directory not exists");
    }

    /** */
    public SnapshotMetadata metadata() throws IOException, IgniteCheckedException {
        File[] metas = dumpDir.listFiles(f -> f.getName().endsWith(DUMP_METAFILE_EXT));

        if (metas.length != 1)
            throw new IgniteException("Wrong number of meta files: " + metas.length);

        try (InputStream in = new BufferedInputStream(Files.newInputStream(metas[0].toPath()))) {
            return MarshallerUtils.jdkMarshaller(cctx.igniteInstanceName()).unmarshal(in, U.resolveClassLoader(cctx.config()));
        }
    }

    /** */
    public List<CacheConfiguration<?, ?>> config(int groupId) {
        JdkMarshaller marsh = MarshallerUtils.jdkMarshaller(cctx.igniteInstanceName());

        return Arrays.stream(FilePageStoreManager.cacheDataFiles(dumpGroupDirectory(groupId))).map(f -> {
            try {
                return readCacheData(f, marsh, cctx.config()).config();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }).collect(Collectors.toList());
    }

    /** */
    public DumpIterator iterator(int groupId) throws IOException {
        FileIOFactory ioFactory = new RandomAccessFileIOFactory();

        FileIO dumpFile = ioFactory.create(new File(dumpGroupDirectory(groupId), DUMP_FILE_NAME));

        byte[] fileVerBytes = new byte[Short.BYTES];

        dumpFile.readFully(fileVerBytes, 0, fileVerBytes.length);

        DumpEntrySerializer serializer = DumpEntrySerializer.serializer(U.bytesToShort(fileVerBytes, 0));

        serializer.kernalContext(cctx);

        return new DumpIterator() {
            DumpEntry next;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                advance();

                return next != null;
            }

            /** {@inheritDoc} */
            @Override public DumpEntry next() {
                advance();

                if (next == null)
                    throw new NoSuchElementException();

                DumpEntry next0 = next;

                next = null;

                return next0;
            }

            /** {@inheritDoc} */
            @Override public void close() throws Exception {
                dumpFile.close();
            }

            /** */
            private void advance() {
                if (next != null)
                    return;

                try {
                    next = serializer.read(dumpFile, groupId);
                }
                catch (IOException | IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        };
    }

    /** */
    private File dumpGroupDirectory(int groupId) {
        File[] nodeDirs = new File(dumpDir, "db")
            .listFiles(f -> f.isDirectory()
                && !f.getAbsolutePath().endsWith(DFLT_BINARY_METADATA_PATH)
                && !f.getAbsolutePath().endsWith(DFLT_MARSHALLER_PATH));

        if (nodeDirs.length != 1)
            throw new IgniteException("Wrong number of node directories: " + nodeDirs.length);

        File[] grpDirs = nodeDirs[0].listFiles(f -> {
            if (!f.isDirectory()
                || (!f.getName().startsWith(CACHE_DIR_PREFIX)
                && !f.getName().startsWith(CACHE_GRP_DIR_PREFIX)))
                return false;

            String grpName = f.getName().startsWith(CACHE_DIR_PREFIX)
                ? f.getName().replace(CACHE_DIR_PREFIX, "")
                : f.getName().replace(CACHE_GRP_DIR_PREFIX, "");

            return groupId == CU.cacheId(grpName);
        });

        if (grpDirs.length != 1)
            throw new IgniteException("Wrong number of group directories: " + grpDirs.length);

        return grpDirs[0];
    }
}
