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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

/**
 *
 */
public class MetsatorageSearchRowImpl implements MetastorageSearchRow {
    /** */
    private final String key;
    /** */
    private final long link;

    /**
     * @param key Key.
     * @param link Link.
     */
    public MetsatorageSearchRowImpl(String key, long link) {
        this.key = key;
        this.link = link;
    }

    /** {@inheritDoc} */
    @Override
    public String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override
    public long link() {
        return link;
    }

    /** {@inheritDoc} */
    @Override
    public int hash() {
        return key.hashCode();
    }
}
