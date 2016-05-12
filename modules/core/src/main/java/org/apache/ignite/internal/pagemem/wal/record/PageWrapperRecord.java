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

import java.nio.ByteBuffer;

/**
 *
 */
public class PageWrapperRecord extends PageAbstractRecord {
    /** */
    private FullPageId fullId;

    /** */
    private ByteBuffer pageBuf;

    /**
     * @param pageBuf Page buf.
     */
    public PageWrapperRecord(FullPageId fullId, ByteBuffer pageBuf) {
        this.fullId = fullId;
        this.pageBuf = pageBuf;
    }

    /**
     * @return Page start pointer.
     */
    public ByteBuffer buffer() {
        return pageBuf;
    }

    /**
     * @return Full page ID.
     */
    public FullPageId fullId() {
        return fullId;
    }
}
