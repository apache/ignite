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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/** */
class DistributedMetaStorageUtil {
    /**
     * Common prefix for everything that is going to be written into {@link MetaStorage}. Something that has minimal
     * chance of collision with the existing keys.
     */
    static final String COMMON_KEY_PREFIX = "\u0000";

    /**
     * Prefix for user keys to store in distributed metastorage.
     */
    private static final String KEY_PREFIX = "key-";

    /**
     * Key for history version.
     */
    private static final String HISTORY_VER_KEY = "hist-ver";

    /**
     * Prefix for history items. Each item will be stored using {@code hist-item-<ver>} key.
     */
    private static final String HISTORY_ITEM_KEY_PREFIX = "hist-item-";

    /**
     * Special key indicating that local data for distributied metastorage is inconsistent because of the ungoing
     * update/recovery process. Data associated with the key may be ignored.
     */
    private static final String CLEANUP_GUARD_KEY = "clean";

    /** */
    @Nullable public static byte[] marshal(Serializable val) throws IgniteCheckedException {
        return val == null ? null : JdkMarshaller.DEFAULT.marshal(val);
    }

    /** */
    @Nullable public static Serializable unmarshal(byte[] valBytes) throws IgniteCheckedException {
        return valBytes == null ? null : JdkMarshaller.DEFAULT.unmarshal(valBytes, U.gridClassLoader());
    }

    /** */
    public static String localKey(String globalKey) {
        return localKeyPrefix() + globalKey;
    }

    /** */
    public static String globalKey(String locKey) {
        assert locKey.startsWith(localKeyPrefix()) : locKey;

        return locKey.substring(localKeyPrefix().length());
    }

    /** */
    public static String localKeyPrefix() {
        return COMMON_KEY_PREFIX + KEY_PREFIX;
    }

    /** */
    public static String historyItemKey(long ver) {
        return historyItemPrefix() + ver;
    }

    /** */
    public static long historyItemVer(String histItemKey) {
        assert histItemKey.startsWith(historyItemPrefix());

        return Long.parseLong(histItemKey.substring(historyItemPrefix().length()));
    }

    /** */
    public static String historyItemPrefix() {
        return COMMON_KEY_PREFIX + HISTORY_ITEM_KEY_PREFIX;
    }

    /** */
    public static String historyVersionKey() {
        return COMMON_KEY_PREFIX + HISTORY_VER_KEY;
    }

    /** */
    public static String cleanupGuardKey() {
        return COMMON_KEY_PREFIX + CLEANUP_GUARD_KEY;
    }
}
