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

import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;

/**
 * Tests concurrent read/write operations for {@code org.apache.ignite.internal.MarshallerMappingFileStore}
 */
public class ConcurrentMappingFileReadWriteTest extends GridCommonAbstractTest {
    /** */
    private File workDir;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        workDir = new File(U.workDirectory(null, null) + DFLT_MARSHALLER_PATH);
        workDir.mkdirs();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.delete(workDir);
    }

    /** */
    @Test
    public void testConcurrentMappingReadWrite() throws Exception {
        BinaryBasicIdMapper mapper = new BinaryBasicIdMapper();

        MarshallerMappingFileStore fs = new MarshallerMappingFileStore(
                new StandaloneGridKernalContext(log, null, null),
                workDir
        );

        CountDownLatch latch = new CountDownLatch(1);

        int typeId = mapper.typeId(String.class.getName());
        String typeName = String.class.getName();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            try {
                assertTrue(latch.await(getTestTimeout(), MILLISECONDS));
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < 10; i++)
                fs.writeMapping((byte) 0, typeId, typeName);
        }, 3);

        latch.countDown();

        fut.get(getTestTimeout(), MILLISECONDS);
    }
}
