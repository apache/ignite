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

package org.apache.ignite.internal.processors.cache.checker.util;

import java.util.Comparator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Comparator for {@link KeyCacheObject}.
 */
public class KeyComparator implements Comparator<KeyCacheObject> {
    /** {@inheritDoc} */
    @Override public int compare(KeyCacheObject k1, KeyCacheObject k2) {
        int cmp = Integer.compare(k1.hashCode(), k2.hashCode());

        if (cmp != 0)
            return cmp;

        final byte[] keyBytes1;
        final byte[] keyBytes2;
        try {
            keyBytes1 = k1.valueBytes(null);
            keyBytes2 = k2.valueBytes(null);
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException("Comparable keys exception.", e);
        }

        return U.compareByteArrays(keyBytes1, keyBytes2);
    }
}
