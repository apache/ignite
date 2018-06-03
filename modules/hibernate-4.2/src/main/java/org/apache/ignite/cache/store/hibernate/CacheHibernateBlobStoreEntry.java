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

package org.apache.ignite.cache.store.hibernate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Entry that is used by {@link CacheHibernateBlobStore} implementation.
 * <p>
 * Note that this is a reference implementation for tests only.
 * When running on production systems use concrete key-value types to
 * get better performance.
 */
@Entity
@Table(name = "ENTRIES")
public class CacheHibernateBlobStoreEntry {
    /** Key (use concrete key type in production). */
    @Id
    @Column(length = 65535)
    private byte[] key;

    /** Value (use concrete value type in production). */
    @Column(length = 65535)
    private byte[] val;

    /**
     * Constructor.
     */
    CacheHibernateBlobStoreEntry() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param key Key.
     * @param val Value.
     */
    CacheHibernateBlobStoreEntry(byte[] key, byte[] val) {
        this.key = key;
        this.val = val;
    }

    /**
     * @return Key.
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void setKey(byte[] key) {
        this.key = key;
    }

    /**
     * @return Value.
     */
    public byte[] getValue() {
        return val;
    }

    /**
     * @param val Value.
     */
    public void setValue(byte[] val) {
        this.val = val;
    }
}