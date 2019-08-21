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

package org.apache.ignite.spi.metric;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.ObjIntConsumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.metric.list.MonitoringListExclude;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;

/**
 *
 */
public class MonitoringRowAttributeWalker<R extends MonitoringRow<?>>  {
    /** */
    public interface AttributeVisitor {
        /** */
        public <T> void accept(int idx, String name, Class<T> clazz);

        /** */
        public void acceptBoolean(int idx, String name);

        /** */
        public void acceptChar(int idx, String name);

        /** */
        public void acceptByte(int idx, String name);

        /** */
        public void acceptShort(int idx, String name);

        /** */
        public void acceptInt(int idx, String name);

        /** */
        public void acceptLong(int idx, String name);

        /** */
        public void acceptFloat(int idx, String name);

        /** */
        public void acceptDouble(int idx, String name);
    }

    /** */
    public interface AttributeWithValueVisitor {
        /** */
        public <T> void accept(int idx, String name, Class<T> clazz, T val);

        /** */
        public void acceptBoolean(int idx, String name, boolean val);

        /** */
        public void acceptChar(int idx, String name, char val);

        /** */
        public void acceptByte(int idx, String name, byte val);

        /** */
        public void acceptShort(int idx, String name, short val);

        /** */
        public void acceptInt(int idx, String name, int val);

        /** */
        public void acceptLong(int idx, String name, long val);

        /** */
        public void acceptFloat(int idx, String name, float val);

        /** */
        public void acceptDouble(int idx, String name, double val);
    }

    /** */
    private static final Set<String> SYSTEM_METHODS = new HashSet<>(Arrays.asList("equals", "hashCode", "toString",
        "getClass"));

    /** Class to explore. */
    private Class<R> clazz;

    /**
     * @param clazz Class to explore.
     */
    public MonitoringRowAttributeWalker(Class<R> clazz) {
        this.clazz = clazz;
    }

    /** @return Count of attributes. */
    public int count() {
        final int[] cnt = {0};

        forEachMethod((m, i) -> cnt[0]++);

        return cnt[0];
    }

    /**
     * @param visitor Field visitor.
     */
    public void visitAll(AttributeVisitor visitor) {
        forEachMethod((m, i) -> {
            String name = m.getName();

            Class<?> clazz = m.getReturnType();

            if (!clazz.isPrimitive())
                visitor.accept(i, name, clazz);
            else if (clazz == boolean.class)
                visitor.acceptBoolean(i, name);
            else if (clazz == char.class)
                visitor.acceptChar(i, name);
            else if (clazz == byte.class)
                visitor.acceptByte(i, name);
            else if (clazz == short.class)
                visitor.acceptShort(i, name);
            else if (clazz == int.class)
                visitor.acceptInt(i, name);
            else if (clazz == long.class)
                visitor.acceptLong(i, name);
            else if (clazz == float.class)
                visitor.acceptFloat(i, name);
            else if (clazz == double.class)
                visitor.acceptDouble(i, name);
            else
                throw new IllegalStateException("Unknown type " + clazz.getName());
        });
    }

    /**
     * @param row Row to explore.
     * @param visitor Visitor.
     */
    public void visitAllWithValues(R row, AttributeWithValueVisitor visitor) {
        forEachMethod((m, i) -> {
            String name = m.getName();

            Class<?> clazz = m.getReturnType();

            try {
                if (!clazz.isPrimitive())
                    visitor.accept(i, name, (Class)clazz, m.invoke(row));
                else if (clazz == boolean.class)
                    visitor.acceptBoolean(i, name, (boolean)m.invoke(row));
                else if (clazz == char.class)
                    visitor.acceptChar(i, name, (char)m.invoke(row));
                else if (clazz == byte.class)
                    visitor.acceptByte(i, name, (byte)m.invoke(row));
                else if (clazz == short.class)
                    visitor.acceptShort(i, name, (short)m.invoke(row));
                else if (clazz == int.class)
                    visitor.acceptInt(i, name, (int)m.invoke(row));
                else if (clazz == long.class)
                    visitor.acceptLong(i, name, (long)m.invoke(row));
                else if (clazz == float.class)
                    visitor.acceptFloat(i, name, (float)m.invoke(row));
                else if (clazz == double.class)
                    visitor.acceptDouble(i, name, (double)m.invoke(row));
            }
            catch (InvocationTargetException | IllegalAccessException e) {
                throw new IgniteException(e);
            }
        });
    }

    /**
     * @param c Method consumer.
     */
    private void forEachMethod(ObjIntConsumer<Method> c) {
        Method[] methods = clazz.getMethods();

        int idx = 0;

        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];

            if (SYSTEM_METHODS.contains(method.getName()))
                continue;

            Class<?> clazz = method.getReturnType();

            if (clazz == void.class)
                continue;

            if (method.getAnnotation(MonitoringListExclude.class) != null)
                continue;

            c.accept(method, idx++);
        }
    }
}
