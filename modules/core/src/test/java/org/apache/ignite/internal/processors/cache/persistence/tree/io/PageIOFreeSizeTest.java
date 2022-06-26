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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.util.IgniteUtils.KB;

/**
 * Tests {@link PageIO#getFreeSpace(int, long)} method for different {@link PageIO} implementations.
 */
@RunWith(Parameterized.class)
public class PageIOFreeSizeTest extends GridCommonAbstractTest {
    /** Page size. */
    @Parameterized.Parameter
    public int pageSz;

    /** */
    @Parameterized.Parameters(name = "pageSz={0}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (long pageSz : new long[] {4 * KB, 8 * KB, 16 * KB})
            params.add(new Object[] {pageSz});

        return params;
    }

    /** Page buffer. */
    private ByteBuffer buf;

    /** Page address. */
    private long addr;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        buf = GridUnsafe.allocateBuffer(pageSz);
        addr = GridUnsafe.bufferAddress(buf);
    }
}
