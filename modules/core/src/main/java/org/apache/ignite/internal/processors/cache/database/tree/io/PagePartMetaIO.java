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
 *
 */

package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;

public class PagePartMetaIO extends PageIO {
    /** */
    private static final int SIZE_OFF = PageIO.COMMON_HEADER_END;

    /** */
    private static final int UPDATE_CNTR_OFF = SIZE_OFF + 8;

    /** */
    private static final int GLOBAL_RMV_ID_OFF = UPDATE_CNTR_OFF + 8;

    /** */
    public static final IOVersions<PagePartMetaIO> VERSIONS = new IOVersions<>(
        new PagePartMetaIO(1)
    );

    public PagePartMetaIO(int ver) {
        super(T_PART_META, ver);
    }

    public long getSize(ByteBuffer buf) {
        return buf.getLong(SIZE_OFF);
    }

    public void setSize(ByteBuffer buf, long size) {
        buf.putLong(SIZE_OFF, size);
    }

    public long getUpdateCounter(ByteBuffer buf) {
        return buf.getLong(UPDATE_CNTR_OFF);
    }

    public void setUpdateCounter(ByteBuffer buf, long cntr) {
        buf.putLong(UPDATE_CNTR_OFF, cntr);
    }

    public long getGlobalRemoveId(ByteBuffer buf) {
        return buf.getLong(GLOBAL_RMV_ID_OFF);
    }

    public void setGlobalRemoveId(ByteBuffer buf, long rmvId) {
        buf.putLong(GLOBAL_RMV_ID_OFF, rmvId);
    }
}
