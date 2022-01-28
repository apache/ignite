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

package org.apache.ignite.internal.pagememory;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.io.PageIoModule;

/**
 * Test implementation of {@link PageIoModule}.
 */
public class TestPageIoModule implements PageIoModule {
    /** Last possible value for IO type. */
    public static final int TEST_PAGE_TYPE = PageIo.MAX_IO_TYPE;

    /** Version 1, minimal possible value. */
    public static final int TEST_PAGE_VER = 1;

    /** {@inheritDoc} */
    @Override
    public Collection<IoVersions<?>> ioVersions() {
        return List.of(new IoVersions<>(new TestPageIo()));
    }

    /**
     * Test implementation of {@link PageIo}.
     */
    public static class TestPageIo extends PageIo {
        /**
         * Constructor.
         */
        public TestPageIo() {
            super(TEST_PAGE_TYPE, TEST_PAGE_VER);
        }

        /** {@inheritDoc} */
        @Override
        protected void printPage(long addr, int pageSize, StringBuilder sb) {
        }
    }
}
