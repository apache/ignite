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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class RootPage {
    /** */
    @GridToStringInclude
    private FullPageId pageId;

    /** */
    private boolean allocated;

    /**
     * @param pageId Page ID.
     * @param allocated {@code True} if was allocated new page.
     */
    public RootPage(final FullPageId pageId, final boolean allocated) {
        this.pageId = pageId;
        this.allocated = allocated;
    }

    /**
     * @return Page ID.
     */
    public FullPageId pageId() {
        return pageId;
    }

    /**
     * @return {@code True} if was allocated.
     */
    public boolean isAllocated() {
        return allocated;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RootPage.class, this);
    }
}
