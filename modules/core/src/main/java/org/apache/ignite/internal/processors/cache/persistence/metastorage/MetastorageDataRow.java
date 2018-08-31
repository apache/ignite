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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.Storable;

/**
 *
 */
public class MetastorageDataRow implements MetastorageSearchRow, Storable {
    /** */
    private long link;

    /** */
    private String key;

    /** */
    private byte[] value;

    /** */
    public MetastorageDataRow(long link, String key, byte[] value) {
        this.link = link;
        this.key = key;
        this.value = value;
    }

    /** */
    public MetastorageDataRow(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    /**
     * @return Key.
     */
    @Override public String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override
    public int hash() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @Override
    public int partition() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int size() throws IgniteCheckedException {
        return 4 + value().length;
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public void link(long link) {
        this.link = link;
    }

    /** {@inheritDoc} */
    @Override
    public long link() {
        return link;
    }

    /**
     * @return Value.
     */
    public byte[] value() {
        return value;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "key=" + key;
    }
}
