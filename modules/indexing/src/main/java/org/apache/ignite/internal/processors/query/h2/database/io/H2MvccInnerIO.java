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

package org.apache.ignite.internal.processors.query.h2.database.io;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/**
 * Inner page for H2 row references.
 */
public class H2MvccInnerIO extends AbstractH2InnerIO {
    /** */
    public static final IOVersions<H2MvccInnerIO> VERSIONS = new IOVersions<>(
        new H2MvccInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private H2MvccInnerIO(int ver) {
        super(T_H2_MVCC_REF_INNER, ver, 28);
    }

    /** {@inheritDoc} */
    @Override public long getMvccCoordinatorVersion(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 8);
    }

    /** {@inheritDoc} */
    @Override public long getMvccCounter(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 16);
    }

    /** {@inheritDoc} */
    @Override public int getMvccOperationCounter(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + 24);
    }

    /** {@inheritDoc} */
    @Override public boolean storeMvccInfo() {
        return true;
    }
}
