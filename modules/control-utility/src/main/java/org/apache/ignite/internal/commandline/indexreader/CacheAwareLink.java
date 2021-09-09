/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
