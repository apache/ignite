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

package org.apache.ignite.internal.binary;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.binary.BinaryObjectException;

/**
 * Cache for enum constants.
 */
public class BinaryEnumCache {
    /** Cache for enum constants. */
    private static final ConcurrentMap<Class<?>, Object[]> ENUM_CACHE = new ConcurrentHashMap<>();

    /**
     * Get value for the given class and ordinal.
     *
     * @param cls Class.
     * @param ord Ordinal.
     * @return Value.
     * @throws BinaryObjectException In case of invalid ordinal.
     */
    public static <T> T get(Class<?> cls, int ord) throws BinaryObjectException {
        assert cls != null;

        if (ord >= 0) {
            Object[] vals = ENUM_CACHE.get(cls);

            if (vals == null) {
                vals = cls.getEnumConstants();

                ENUM_CACHE.putIfAbsent(cls, vals);
            }

            if (ord < vals.length)
                return (T) vals[ord];
            else
                throw new BinaryObjectException("Failed to get enum value for ordinal (do you have correct class " +
                    "version?) [cls=" + cls.getName() + ", ordinal=" + ord + ", totalValues=" + vals.length + ']');
        }
        else
            return null;
    }

    /**
     * Clears cache.
     */
    public static void clear() {
        ENUM_CACHE.clear();
    }
}
