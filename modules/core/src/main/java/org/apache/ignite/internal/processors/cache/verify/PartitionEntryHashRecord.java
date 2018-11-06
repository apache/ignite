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
package org.apache.ignite.internal.processors.cache.verify;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class PartitionEntryHashRecord implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache id. */
    @GridToStringExclude
    private final int cacheId;

    /** Cache name. */
    private final String cacheName;

    /** Binary key. */
    @GridToStringExclude
    private final KeyCacheObject key;

    /** Key string. */
    @GridToStringExclude
    private final String keyStr;

    /** Key bytes. */
    private final byte[] keyBytes;

    /** Grid Cache Version. */
    private final GridCacheVersion ver;

    /** Value. */
    @GridToStringExclude
    private volatile CacheObject val;

    /** Value bytes. */
    private volatile byte[] valBytes;

    /** Value string. */
    @GridToStringExclude
    private volatile String valStr;

    /** Value hash. */
    @GridToStringExclude
    private final int valHash;

    /**
     * @param cacheId Cache id.
     * @param cacheName Cache name.
     * @param key Key.
     * @param keyStr Key string.
     * @param keyBytes Key bytes.
     * @param ver Version.
     * @param valHash Value hash.
     * @param val Value.
     * @param valStr Value string.
     * @param valBytes Value bytes.
     */
    public PartitionEntryHashRecord(int cacheId, String cacheName, KeyCacheObject key, String keyStr,
        byte[] keyBytes, GridCacheVersion ver, int valHash, CacheObject val, String valStr, byte[] valBytes) {
        this.cacheId = cacheId;
        this.cacheName = cacheName;
        this.key = key;
        this.keyStr = keyStr;
        this.keyBytes = keyBytes;
        this.ver = ver;
        this.val = val;
        this.valStr = valStr;
        this.valHash = valHash;
        this.valBytes = valBytes;
    }

    /**
     * @param cacheId Cache id.
     * @param cacheName Cache name.
     * @param key Key.
     * @param keyStr Key string.
     * @param keyBytes Key bytes.
     * @param ver Version.
     * @param valHash Value hash.
     */
    public PartitionEntryHashRecord(int cacheId, String cacheName, KeyCacheObject key,
        String keyStr, byte[] keyBytes, GridCacheVersion ver, int valHash) {
        this(cacheId, cacheName, key, keyStr, keyBytes, ver, valHash, null, null, null);
    }

    /**
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Binary key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Key bytes.
     */
    public byte[] keyBytes() {
        return keyBytes;
    }

    /**
     * @return Grid Cache Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Value hash.
     */
    public int valueHash() {
        return valHash;
    }

    /**
     * @return Key string.
     */
    public String keyString() {
        return keyStr;
    }

    /**
     * @return Binary value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @param val Value.
     */
    public void value(CacheObject val) {
        this.val = val;
    }

    /**
     * @return Value string.
     */
    public String valueString() {
        return valStr;
    }

    /**
     * @param valStr Value string.
     */
    public void valueString(String valStr) {
        this.valStr = valStr;
    }

    /**
     * @param valBytes Value bytes.
     */
    public void valueBytes(byte[] valBytes) {
        this.valBytes = valBytes;
    }

    /**
     * @return Value bytes.
     */
    public byte[] valueBytes() {
        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionEntryHashRecord that = (PartitionEntryHashRecord)o;

        if (cacheId != that.cacheId)
            return false;
        if (valHash != that.valHash)
            return false;
        if (!Arrays.equals(keyBytes, that.keyBytes))
            return false;
        return ver != null ? ver.equals(that.ver) : that.ver == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = cacheId;
        res = 31 * res + Arrays.hashCode(keyBytes);
        res = 31 * res + (ver != null ? ver.hashCode() : 0);
        res = 31 * res + valHash;
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionEntryHashRecord.class, this,
            "key", keyStr,
            "value", valStr,
            "keyBytes", keyBytes != null ? U.byteArray2HexString(keyBytes) : null,
            "valueBytes", valBytes != null ? U.byteArray2HexString(valBytes) : null);
    }
}
