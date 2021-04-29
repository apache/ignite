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
package org.apache.ignite.internal.commandline.indexreader;

/**
 * Link that is representing page id with offset and cache id.
 */
class CacheAwareLink {
    /** */
    public final int cacheId;

    /** */
    public final long link;

    /** True if a link points to tombstone value. */
    public final boolean tombstone;

    /** */
    public CacheAwareLink(int cacheId, long link, boolean tombstone) {
        this.cacheId = cacheId;
        this.link = link;
        this.tombstone = tombstone;
    }
}
