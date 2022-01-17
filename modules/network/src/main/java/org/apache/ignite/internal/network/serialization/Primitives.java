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

package org.apache.ignite.internal.network.serialization;

/**
 * Utils to work with primitives.
 */
public class Primitives {
    /**
     * Returns number of bytes a value of the given primtive type takes (1 for byte, 8 for long and so on).
     *
     * @param clazz primitive type
     * @return number of bytes
     * @throws IllegalArgumentException if the passed type is not primitive
     */
    public static int widthInBytes(Class<?> clazz) {
        if (clazz == byte.class) {
            return Byte.BYTES;
        } else if (clazz == short.class) {
            return Short.BYTES;
        } else if (clazz == int.class) {
            return Integer.BYTES;
        } else if (clazz == long.class) {
            return Long.BYTES;
        } else if (clazz == float.class) {
            return Float.BYTES;
        } else if (clazz == double.class) {
            return Double.BYTES;
        } else if (clazz == char.class) {
            return Character.BYTES;
        } else if (clazz == boolean.class) {
            return 1;
        } else {
            throw new IllegalArgumentException(clazz + " is not primitive");
        }
    }
}
