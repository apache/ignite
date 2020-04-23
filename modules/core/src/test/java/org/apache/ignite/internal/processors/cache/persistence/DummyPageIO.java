/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CompactablePageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Dummy PageIO implementation. For test purposes only.
 */
public class DummyPageIO extends PageIO implements CompactablePageIO {
    /** */
    public static final IOVersions<DummyPageIO> VERSIONS = new IOVersions<>(new DummyPageIO());

    /** */
    public DummyPageIO() {
        super(2 * Short.MAX_VALUE, 1);

        PageIO.registerTest(this);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("DummyPageIO [\n");
        sb.a("addr=").a(addr).a(", ");
        sb.a("pageSize=").a(addr);
        sb.a("\n]");
    }

    /** {@inheritDoc} */
    @Override public void compactPage(ByteBuffer page, ByteBuffer out, int pageSize) {
        copyPage(page, out, pageSize);
    }

    /** {@inheritDoc} */
    @Override public void restorePage(ByteBuffer p, int pageSize) {
        assert p.isDirect();
        assert p.position() == 0;
        assert p.limit() == pageSize;
    }
}
