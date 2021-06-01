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

package org.apache.ignite.internal.cache.query.index.sorted.inline.io;

import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inner page for inlined MVCC index rows.
 */
public class MvccInlineInnerIO extends AbstractInlineInnerIO {
    /**
     * @param type Page type.
     * @param payloadSize Payload size.
     */
    protected MvccInlineInnerIO(short type, int payloadSize) {
        // Meta stores link and mvcc info about a cache row.
        super(type, 1, 28, payloadSize);
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + inlineSize() + 8);
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + inlineSize() + 16);
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + inlineSize() + 24);
    }

    /** {@inheritDoc} */
    @Override public boolean storeMvccInfo() {
        return true;
    }
}

