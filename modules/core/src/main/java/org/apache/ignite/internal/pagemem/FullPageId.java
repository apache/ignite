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

package org.apache.ignite.internal.pagemem;

/**
 *
 */
public class FullPageId {
    /** */
    private long pageId;

    /** */
    private int cacheId;

    /**
     * @param pageId Page ID.
     * @param cacheId Cache ID.
     */
    public FullPageId(long pageId, int cacheId) {
        this.pageId = pageId;
        this.cacheId = cacheId;
    }

    /**
     * @return Page ID.
     */
    public long pageId() {
        return pageId;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof FullPageId))
            return false;

        FullPageId that = (FullPageId)o;

        return pageId == that.pageId && cacheId == that.cacheId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = (int)(pageId ^ (pageId >>> 32));

        result = 31 * result + cacheId;

        return result;
    }
}
