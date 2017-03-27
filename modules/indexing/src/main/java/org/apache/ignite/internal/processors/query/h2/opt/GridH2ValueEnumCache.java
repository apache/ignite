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
package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.IgniteCheckedException;
import org.jsr166.ConcurrentHashMap8;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;

public class GridH2ValueEnumCache {
    private static final ConcurrentMap<Class<?>, GridH2ValueEnum[]> VALS = new ConcurrentHashMap8<>();
    private static final ConcurrentMap<Class<?>, HashMap<String, GridH2ValueEnum>> NAMES = new ConcurrentHashMap8<>();

    public static GridH2ValueEnum get(int typeId, Class<?> cls, int ord) throws IgniteCheckedException {
        assert cls != null;
        assert ord >= 0;

        GridH2ValueEnum[] vals = VALS.get(cls);

        if (vals == null) {
            Object[] constants = cls.getEnumConstants();
            vals = new GridH2ValueEnum[constants.length];
            for (Object o: constants) {
                Enum e = (Enum)o;
                vals[e.ordinal()] = new GridH2ValueEnum(typeId, cls, e.ordinal(), e.name());
            }
            VALS.putIfAbsent(cls, vals);
        }

        if (ord < vals.length)
            return vals[ord];
        else
            throw new IgniteCheckedException("Failed to get H2 enum value for ordinal (do you have correct class " +
                "version?) [cls=" + cls.getName() + ", ordinal=" + ord + ", totalValues=" + vals.length + ']');
    }

    public static GridH2ValueEnum get(int typeId, Class<?> cls, String name) throws IgniteCheckedException {
        assert cls != null;
        assert name != null;

        HashMap<String, GridH2ValueEnum> names = NAMES.get(cls);

        if (names == null) {
            Object[] constants = cls.getEnumConstants();
            names = new HashMap<>(constants.length);
            for (Object o: constants) {
                Enum e = (Enum)o;
                names.put(e.name(), new GridH2ValueEnum(typeId, cls, e.ordinal(), e.name()));
            }
            NAMES.putIfAbsent(cls, names);
        }

        GridH2ValueEnum value = names.get(name.toUpperCase());
        if (value != null)
            return value;
        else
            throw new IgniteCheckedException("Failed to get H2 enum value for string constant (do you have correct class " +
                    "version?) [cls=" + cls.getName() + ", name=" + name+ ", names=" + Arrays.toString(names.keySet().toArray()) + ']');
    }


}
