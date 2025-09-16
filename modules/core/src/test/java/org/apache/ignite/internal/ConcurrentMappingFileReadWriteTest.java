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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Tests concurrent read/write operations for {@code org.apache.ignite.internal.MarshallerMappingFileStore}.
 */
public class ConcurrentMappingFileReadWriteTest extends GridCommonAbstractTest {
    /** */
    private static final byte PLATFORM_ID = (byte)0;

    /** */
    private static final int TYPE_ID = new BinaryBasicIdMapper().typeId(String.class.getName());

    /** */
    private File mappingDir;

    /** */
    private MarshallerMappingFileStore mappingFileStore;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        mappingDir = sharedFileTree().mkdirMarshaller();

        mappingFileStore = new MarshallerMappingFileStore(
            new StandaloneGridKernalContext(log, null),
            mappingDir
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.delete(mappingDir);
    }

    /** */
    @Test
    public void testConcurrentMappingReadWrite() throws Exception {
        int thCnt = 3;

        CyclicBarrier barrier = new CyclicBarrier(thCnt);

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            try {
                barrier.await(getTestTimeout(), MILLISECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }

            mappingFileStore.writeMapping(PLATFORM_ID, TYPE_ID, String.class.getName());

            for (int i = 0; i < 10; i++) {
                assertEquals(String.class.getName(), mappingFileStore.readMapping(PLATFORM_ID, TYPE_ID));

                mappingFileStore.writeMapping(PLATFORM_ID, TYPE_ID, String.class.getName());
            }
        }, thCnt);

        fut.get(getTestTimeout(), MILLISECONDS);
    }

    /** */
    @Test
    public void testRewriteOpenedFile() throws Exception {
        mappingFileStore.writeMapping(PLATFORM_ID, TYPE_ID, String.class.getName());

        File mappingFile = new File(mappingDir, BinaryUtils.mappingFileName(PLATFORM_ID, TYPE_ID));

        try (FileInputStream in = new FileInputStream(mappingFile)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                // Rewrite file while it open. This can happen if CDC application read newly created file.
                mappingFileStore.writeMapping(PLATFORM_ID, TYPE_ID, String.class.getName());

                assertEquals(String.class.getName(), reader.readLine());
            }
        }
    }
}
