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

import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/**
 *
 */
public class MetastorageDataRow extends SimpleDataRow implements MetastorageSearchRow {
    /** */
    private String key;

    /** */
    public MetastorageDataRow(long link, String key, byte[] val) {
        super(link, MetaStorage.PRESERVE_LEGACY_METASTORAGE_PARTITION_ID ?
            PageIdAllocator.OLD_METASTORE_PARTITION: PageIdAllocator.METASTORE_PARTITION, val);
        this.key = key;
    }

    /** */
    public MetastorageDataRow(String key, byte[] val) {
        this(0, key, val);
    }

    /**
     * @return Key.
     */
    @Override public String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @Override public IOVersions<? extends AbstractDataPageIO> ioVersions() {
        return MetastoreDataPageIO.VERSIONS;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "key=" + key;
    }
}
