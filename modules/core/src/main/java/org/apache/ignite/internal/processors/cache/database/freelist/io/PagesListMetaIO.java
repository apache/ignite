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

package org.apache.ignite.internal.processors.cache.database.freelist.io;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;

/**
 *
 */
public class PagesListMetaIO extends PageIO {
    /** */
    public static final IOVersions<PagesListMetaIO> VERSIONS = new IOVersions<>(
        new PagesListMetaIO(1)
    );

    /**
     * @param ver  Page format version.
     */
    protected PagesListMetaIO(int ver) {
        super(T_PAGE_LIST_META, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setCount(buf, 0);
        setNextMetaPageId(buf, 0L);
    }

    private int getCount(ByteBuffer buf) {
        return 0;
    }

    private void setCount(ByteBuffer buf, int cnt) {

    }

    public long getNextMetaPageId(ByteBuffer buf) {
        return 0L;
    }

    public void setNextMetaPageId(ByteBuffer buf, long metaPageId) {

    }

    public boolean addListHead(int bucket, long headId, long tailId) {
        return false;
    }
}
