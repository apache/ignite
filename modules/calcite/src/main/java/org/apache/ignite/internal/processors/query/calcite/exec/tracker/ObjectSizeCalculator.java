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

package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.ToLongBiFunction;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.ignite.internal.binary.BinaryArray;
import org.apache.ignite.internal.binary.BinaryEnumArray;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.IterableAccumulator;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Object size calculator.
 * Not thread-safe.
 */
public class ObjectSizeCalculator<Row> {
    /** Calculators for most commonly used classes, without reflection. */
    private static final Map<Class<?>, SizeCalculator<?>> SYS_CLS_SIZE = new IdentityHashMap<>();

    /** Object header size. */
    private static final long OBJ_HEADER_SIZE = GridUnsafe.objectFieldOffset(U.findField(Dummy.class, "field"));

    /** Reference type size. */
    public static final long OBJ_REF_SIZE = GridUnsafe.arrayIndexScale(Object[].class);

    /** Objects allignment. */
    private static final long OBJ_ALIGN = calcAlign();

    /** HashMap entry size. */
    public static final long HASH_MAP_ENTRY_SIZE =
        classInfo(new HashMap<>(F.asMap(0, 0)).entrySet().iterator().next().getClass()).shallowSize;

    /** Cache for shallow size of classes. */
    private final Map<Class<?>, ClassInfo> clsInfoCache = new IdentityHashMap<>();

    /** */
    private static long calcAlign() {
        // Note: Alignment can also be set explicitly by -XX:ObjectAlignmentInBytes JVM property.
        return OBJ_REF_SIZE == 8L ? 8L :
            U.nearestPow2(Math.max(8, (int)(Runtime.getRuntime().maxMemory() >> 32)), false);
    }

    static {
        addSysClsSize(Boolean.class, null);
        addSysClsSize(Byte.class, null);
        addSysClsSize(Character.class, null);
        addSysClsSize(Short.class, null);
        addSysClsSize(Integer.class, null);
        addSysClsSize(Float.class, null);
        addSysClsSize(Long.class, null);
        addSysClsSize(Double.class, null);
        long charArrOffset = GridUnsafe.arrayBaseOffset(char[].class);
        addSysClsSize(String.class, (c, s) -> align(charArrOffset + ((long)s.length()) * Character.BYTES));
        long byteArrOffset = GridUnsafe.arrayBaseOffset(byte[].class);
        addSysClsSize(ByteString.class, (c, s) -> align(byteArrOffset + s.length()));

        // Date/time.
        // Assume dates are constructed by millis and "cdate" field is null.
        addSysClsSize(java.util.Date.class, null);
        addSysClsSize(Date.class, null);
        addSysClsSize(Time.class, null);
        addSysClsSize(Timestamp.class, null);

        addSysClsSize(LocalDate.class, null);
        addSysClsSize(LocalTime.class, null);
        long locDateTimeExtraSize = classInfo(LocalDate.class).shallowSize + classInfo(LocalTime.class).shallowSize;
        addSysClsSize(LocalDateTime.class, (c, dt) -> locDateTimeExtraSize);

        addSysClsSize(Period.class, null);
        addSysClsSize(Duration.class, null);

        // Binary objects.
        // Assume objects is not deserialized.
        addSysClsSize(BinaryObjectImpl.class, (c, bo) -> align(byteArrOffset + bo.array().length));
        addSysClsSize(BinaryObjectOffheapImpl.class, null); // No extra heap memory.
        addSysClsSize(BinaryArray.class, (c, bo) -> c.sizeOf0(bo.array(), true));
        addSysClsSize(BinaryEnumArray.class, (c, bo) -> c.sizeOf0(bo.array(), true));
        addSysClsSize(BinaryEnumObjectImpl.class, (c, bo) -> bo.size());

        // Other.
        addSysClsSize(GroupKey.class, (c, k) -> c.sizeOf0(k.fields(), true));
        addSysClsSize(UUID.class, null);
        addSysClsSize(BigDecimal.class, (c, bd) -> c.sizeOf0(bd.unscaledValue(), true));
        long intArrOffset = GridUnsafe.arrayBaseOffset(int[].class);
        addSysClsSize(BigInteger.class, (c, bi) -> align( intArrOffset + ((bi.bitLength() + 31) >> 5) << 2));
        SYS_CLS_SIZE.put(Class.class, (c, v) -> 0);
    }

    /** */
    private static <T> void addSysClsSize(Class<T> cls, @Nullable ToLongBiFunction<ObjectSizeCalculator<?>, T> extraSizeCalc) {
        SYS_CLS_SIZE.put(cls, new SizeCalculatorImpl<>(cls, extraSizeCalc));
    }

    /** */
    private final Map<Object, Object> processedObjs = new IdentityHashMap<>();

    /** */
    private static ClassInfo classInfo(Class<?> cls) {
        long size = 0;

        List<Field> refFields = new ArrayList<>();

        for (; cls != null && cls != Object.class; cls = cls.getSuperclass()) {
            for (Field f : cls.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers()))
                    continue;

                size = Math.max(size, GridUnsafe.objectFieldOffset(f) + fieldHolderSize(f));

                if (f.getDeclaringClass().isPrimitive())
                    continue;

                if (!f.isAccessible())
                    f.setAccessible(true);

                refFields.add(f);
            }
        }

        return new ClassInfo(align(Math.max(size, OBJ_HEADER_SIZE)), refFields);
    }

    /** Field holder size. */
    private static long fieldHolderSize(Field field) {
        Class<?> cls = field.getDeclaringClass();

        if (cls.isPrimitive()) {
            if (cls == boolean.class || cls == byte.class)
                return 1L;
            if (cls == char.class || cls == short.class)
                return 2L;
            if (cls == int.class || cls == float.class)
                return 4L;
            if (cls == long.class || cls == double.class)
                return 8L;
        }

        return OBJ_REF_SIZE;
    }

    /** Calculate size with alignment. */
    private static long align(long size) {
        return (size + (OBJ_ALIGN - 1L)) & (-OBJ_ALIGN);
    }

    /** */
    private ClassInfo cachedClassInfo(Class<?> cls) {
        ClassInfo clsInfo = clsInfoCache.get(cls);

        if (clsInfo == null) {
            clsInfo = classInfo(cls);

            clsInfoCache.put(cls, clsInfo);
        }

        return clsInfo;
    }

    /**
     * Estimate size of the row.
     *
     * @param row Row.
     */
    public long sizeOf(Row row) {
        long res = sizeOf0(row, true); // Assume row cannot be refered by it's fields.

        if (!processedObjs.isEmpty())
            processedObjs.clear();

        return res;
    }

    /**
     * @param obj Object.
     * @param objNotReferred {@code True} if we know for sure that object cannot be referred from any fields.
     */
    private long sizeOf0(Object obj, boolean objNotReferred) {
        if (obj == null)
            return 0L;

        Class<?> cls = obj.getClass();

        SizeCalculator<Object> sizeCalc = (SizeCalculator<Object>)SYS_CLS_SIZE.get(cls);

        if (sizeCalc != null)
            return sizeCalc.calcSize(this, obj);

        if (cls.isEnum())
            return 0L;

        // Skip hash map check for objects that cannot be referred.
        if (!objNotReferred && processedObjs.put(obj, obj) != null)
            return 0L;

        if (cls.isArray()) {
            long base = GridUnsafe.arrayBaseOffset(cls);
            long scale = GridUnsafe.arrayIndexScale(cls);
            long size = align(base + (scale * Array.getLength(obj)));

            if (!cls.getComponentType().isPrimitive()) {
                Object[] arr = (Object[])obj;

                for (int i = 0; i < arr.length; i++)
                    size += sizeOf0(arr[i], false);
            }

            return size;
        }

        // Short-circuit for accumulators containing rows.
        if (IterableAccumulator.class.isAssignableFrom(cls)) {
            long size = cachedClassInfo(cls).shallowSize;
            IterableAccumulator<Row> it = (IterableAccumulator<Row>)obj;

            for (Row row : it)
                size += sizeOf0(row, true);

            return size;
        }

        return reflectiveObjectSize(obj);
    }

    /** Calculate object size using reflection and recursive calls. */
    private long reflectiveObjectSize(Object obj) {
        Class<?> cls = obj.getClass();

        ClassInfo clsInfo = cachedClassInfo(cls);

        long size = clsInfo.shallowSize;

        for (Field f : clsInfo.refFields) {
            try {
                size += sizeOf0(f.get(obj), false);
            }
            catch (IllegalAccessException ignore) {
                // Cannot calculate, ignore.
            }
        }

        return size;
    }

    /** Dummy class to calculate Object header size. */
    @SuppressWarnings("unused")
    private static class Dummy {
        /** */
        private byte field;
    }

    /** */
    private static interface SizeCalculator<T> {
        /** Calculate size of the object. */
        public long calcSize(ObjectSizeCalculator<?> calculator, T val);
    }

    /** */
    private static class SizeCalculatorImpl<T> implements SizeCalculator<T> {
        /** */
        private final long shallowSize;

        /** */
        @Nullable private final ToLongBiFunction<ObjectSizeCalculator<?>, T> extraSizeCalc;

        /** */
        private SizeCalculatorImpl(Class<T> cls, @Nullable ToLongBiFunction<ObjectSizeCalculator<?>, T> extraSizeCalc) {
            shallowSize = classInfo(cls).shallowSize;
            this.extraSizeCalc = extraSizeCalc;
        }

        /** {@inheritDoc} */
        @Override public long calcSize(ObjectSizeCalculator<?> calculator, T val) {
            return extraSizeCalc == null ? shallowSize : shallowSize + extraSizeCalc.applyAsLong(calculator, val);
        }
    }

    /** */
    private static class ClassInfo {
        /** */
        private final long shallowSize;

        /** */
        private final List<Field> refFields;

        /** */
        private ClassInfo(long shallowSize, List<Field> refFields) {
            this.shallowSize = shallowSize;
            this.refFields = refFields;
        }
    }
}
