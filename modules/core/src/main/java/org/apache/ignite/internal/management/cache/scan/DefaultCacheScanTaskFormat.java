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

package org.apache.ignite.internal.management.cache.scan;

import java.util.Arrays;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Default cache scan task format.
 */
public class DefaultCacheScanTaskFormat implements CacheScanTaskFormat {
    /** Key title. */
    public static final String KEY = "Key";

    /** Value title. */
    public static final String VALUE = "Value";

    /** {@inheritDoc} */
    @Override public String name() {
        return "default";
    }

    /** {@inheritDoc} */
    @Override public List<String> titles(Cache.Entry<Object, Object> first) {
        return Arrays.asList("Key Class", KEY, "Value Class", VALUE);
    }

    /** {@inheritDoc} */
    @Override public List<?> row(Cache.Entry<Object, Object> e) {
        Object k = e.getKey();
        Object v = e.getValue();

        return Arrays.asList(typeOf(k), valueOf(k), typeOf(v), valueOf(v));
    }

    /**
     * @param o Source object.
     * @return String representation of object class.
     */
    private static String typeOf(Object o) {
        if (o != null) {
            Class<?> clazz = o.getClass();

            return clazz.isArray() ? IgniteUtils.compact(clazz.getComponentType().getName()) + "[]"
                : IgniteUtils.compact(o.getClass().getName());
        }
        else
            return "n/a";
    }

    /**
     * @param o Object.
     * @return String representation of value.
     */
    static String valueOf(Object o) {
        if (o == null)
            return "null";

        if (o instanceof byte[]) {
            return "size=" + ((byte[])o).length;
        }
        else if (o instanceof Byte[]) {
            return "size=" + ((Byte[])o).length;
        }
        else if (o instanceof boolean[]) {
            boolean[] arr = (boolean[])o;
            return arrayValue(arr.length, Arrays.toString(arr));
        }
        else if (o instanceof char[]) {
            char[] arr = (char[])o;
            return arrayValue(arr.length, Arrays.toString(arr));
        }
        else if (o instanceof short[]) {
            short[] arr = (short[])o;
            return arrayValue(arr.length, Arrays.toString(arr));
        }
        else if (o instanceof int[]) {
            int[] arr = (int[])o;
            return arrayValue(arr.length, Arrays.toString(arr));
        }
        else if (o instanceof long[]) {
            long[] arr = (long[])o;
            return arrayValue(arr.length, Arrays.toString(arr));
        }
        else if (o instanceof float[]) {
            float[] arr = (float[])o;
            return arrayValue(arr.length, Arrays.toString(arr));
        }
        else if (o instanceof double[]) {
            double[] arr = (double[])o;
            return arrayValue(arr.length, Arrays.toString(arr));
        }
        else if (o instanceof Object[]) {
            return arrayValue(((Object[])o).length, "[" + S.joinToString(Arrays.asList((Object[])o), ", ", "...", 120, 0)) + "]";
        }

        if (o instanceof BinaryObject)
            return binaryToString((BinaryObject)o);

        return o.toString();
    }

    /** */
    static String arrayValue(int length, String values) {
        return "size=" + length + ", values=" + values;
    }

    /**
     * Convert Binary object to string.
     *
     * @param obj Binary object.
     * @return String representation of Binary object.
     */
    public static String binaryToString(BinaryObject obj) {
        int hash = obj.hashCode();

        if (obj instanceof BinaryObjectEx) {
            BinaryObjectEx objEx = (BinaryObjectEx)obj;

            BinaryType meta;

            try {
                meta = ((BinaryObjectEx)obj).rawType();
            }
            catch (BinaryObjectException ignore) {
                meta = null;
            }

            if (meta != null) {
                if (meta.isEnum()) {
                    try {
                        return obj.deserialize().toString();
                    }
                    catch (BinaryObjectException ignore) {
                        // NO-op.
                    }
                }

                SB buf = new SB(meta.typeName());

                if (meta.fieldNames() != null) {
                    buf.a(" [hash=").a(hash);

                    for (String name : meta.fieldNames()) {
                        Object val = objEx.field(name);

                        buf.a(", ").a(name).a('=').a(val);
                    }

                    buf.a(']');

                    return buf.toString();
                }
            }
        }

        return S.toString(obj.getClass().getSimpleName(),
            "hash", hash, false,
            "typeId", obj.type().typeId(), true);
    }

}
