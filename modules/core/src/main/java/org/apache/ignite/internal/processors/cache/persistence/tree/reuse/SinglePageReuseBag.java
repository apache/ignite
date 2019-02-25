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

package org.apache.ignite.internal.processors.cache.persistence.tree.reuse;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public final class SinglePageReuseBag implements ReuseBag {
    /** */
    private long pageId;

    /**
     * @param pageId Page ID.
     */
    public SinglePageReuseBag(long pageId) {
        assert pageId != 0L;

        this.pageId = pageId;
    }

    /** {@inheritDoc} */
    @Override public boolean addFreePage(long pageId) {
        assert pageId != 0L;

        if (this.pageId != 0L)
            return false;

        this.pageId = pageId;
        return true;
    }

    /** {@inheritDoc} */
    @Override public long pollFreePage() {
        long res = pageId;

        pageId = 0L;

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return pageId == 0L;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SinglePageReuseBag.class, this, "pageId", U.hexLong(pageId));
    }
}
