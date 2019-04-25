/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.database.io;

import org.apache.ignite.internal.pagemem.PageUtils;

/**
 *
 */
class H2MvccExtrasInnerIO extends AbstractH2ExtrasInnerIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param payloadSize Payload size.
     */
    H2MvccExtrasInnerIO(short type, int ver, int payloadSize) {
        super(type, ver, 28, payloadSize);
    }

    /** {@inheritDoc} */
    @Override public long getMvccCoordinatorVersion(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + payloadSize + 8);
    }

    /** {@inheritDoc} */
    @Override public long getMvccCounter(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + payloadSize + 16);
    }

    /** {@inheritDoc} */
    @Override public int getMvccOperationCounter(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + payloadSize + 24);
    }

    /** {@inheritDoc} */
    @Override public boolean storeMvccInfo() {
        return true;
    }
}

