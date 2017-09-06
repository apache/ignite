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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class PageSnapshot extends WALRecord {
    /** */
    @GridToStringExclude
    private byte[] pageData;

    /** */
    private FullPageId fullPageId;

    /**
     * @param fullId Full page ID.
     * @param arr Read array.
     */
    public PageSnapshot(FullPageId fullId, byte[] arr) {
        fullPageId = fullId;
        pageData = arr;
    }

    /**
     * @param fullPageId Full page ID.
     * @param ptr Pointer to copy from.
     * @param pageSize Page size.
     */
    public PageSnapshot(FullPageId fullPageId, long ptr, int pageSize) {
        this.fullPageId = fullPageId;

        pageData = new byte[pageSize];

        GridUnsafe.copyMemory(null, ptr, pageData, GridUnsafe.BYTE_ARR_OFF, pageSize);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PAGE_RECORD;
    }

    /**
     * @return Snapshot of page data.
     */
    public byte[] pageData() {
        return pageData;
    }

    /**
     * @return Full page ID.
     */
    public FullPageId fullPageId() {
        return fullPageId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PageSnapshot.class, this, super.toString());
    }
}
