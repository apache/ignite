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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/** */
class DistributedMetaStorageUtil {
    /** */
    static final String COMMON_KEY_PREFIX = "\u0000";

    /** */
    private static final String KEY_PREFIX = "key-";

    /** */
    private static final String HISTORY_VER_KEY = "hist-ver";

    /** */
    private static final String HISTORY_ITEM_KEY_PREFIX = "hist-item-";

    /** */
    private static final String CLEANUP_GUARD_KEY = "clean";

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
