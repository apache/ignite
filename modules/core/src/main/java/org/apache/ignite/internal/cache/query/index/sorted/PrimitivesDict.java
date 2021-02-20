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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.HashMap;
import java.util.Map;

/** Represents dictionary of primitive and wrapper classes. */
public class PrimitivesDict {
    /** */
    private static final Map<Class<?>, Class<?>> primitivesMap = new HashMap<>();

    static {
        primitivesMap.put(boolean.class, Boolean.class);
        primitivesMap.put(byte.class, Byte.class);
        primitivesMap.put(short.class, Short.class);
        primitivesMap.put(char.class, Character.class);
        primitivesMap.put(int.class, Integer.class);
        primitivesMap.put(long.class, Long.class);
        primitivesMap.put(float.class, Float.class);
        primitivesMap.put(double.class, Double.class);
    }

    /** Check whether classes are alternatives in context of this dictionary. */
    public static boolean areAlternatives(Class<?> cls1, Class<?> cls2) {
        return primitivesMap.get(cls1) == cls2 || primitivesMap.get(cls2) == cls1;
    }
}
