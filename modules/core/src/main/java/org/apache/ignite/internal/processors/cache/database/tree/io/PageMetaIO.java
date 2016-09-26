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

package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class PageMetaIO extends PageIO {
    /** */
    protected static final int METASTORE_ROOT_OFF = PageIO.COMMON_HEADER_END;

    /** */
    protected static final int REUSE_LIST_ROOT_OFF = METASTORE_ROOT_OFF + 8;

    /** */
    public static final IOVersions<PageMetaIO> VERSIONS = new IOVersions<>(
        new PageMetaIO(1)
    );

    /**
     * @param ver Page format version.
     */
    public PageMetaIO(int ver) {
        super(PageIO.T_META, ver);
    }

    /**
     * @param type Type.
     * @param ver Version.
     */
    protected PageMetaIO(int type, int ver) {
       super(type, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setTreeRoot(buf, 0);
        setReuseListRoot(buf, 0);
    }

    /**
     * @param buf Buffer.
     * @return Meta store root page.
     */
    public long getTreeRoot(ByteBuffer buf) {
        return buf.getLong(METASTORE_ROOT_OFF);
    }

    /**
     * @param buf Buffer.
     * @param metastoreRoot metastore root
     */
    public void setTreeRoot(@NotNull ByteBuffer buf, long metastoreRoot) {
        buf.putLong(METASTORE_ROOT_OFF, metastoreRoot);
    }

    /**
     * @param buf Buffer.
     * @return Reuse list root page.
     */
    public long getReuseListRoot(ByteBuffer buf) {
        return buf.getLong(REUSE_LIST_ROOT_OFF);
    }

    /**
     * @param buf Buffer.
     * @param pageId Root page ID.
     */
    public void setReuseListRoot(@NotNull ByteBuffer buf, long pageId) {
        buf.putLong(REUSE_LIST_ROOT_OFF, pageId);
    }
}
