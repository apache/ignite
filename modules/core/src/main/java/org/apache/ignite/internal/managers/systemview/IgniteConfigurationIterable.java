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

package org.apache.ignite.internal.managers.systemview;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.TreeMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.ConfigurationView;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Responsibility of this class it to recursively iterate {@link IgniteConfiguration} object
 * and expose all properties in for of String pairs.
 */
public class IgniteConfigurationIterable implements Iterable<ConfigurationView> {
    /** */
    private final Queue<GridTuple3<Object, Iterator<Map.Entry<String, Method>>, String>> iters = new LinkedList<>();

    /**
     * @param cfg Configuration to iterate.
     */
    public IgniteConfigurationIterable(IgniteConfiguration cfg) {
        addToQueue(cfg, "");
    }

    /** {@inheritDoc} */
    @Override public Iterator<ConfigurationView> iterator() {
        return new Iterator<ConfigurationView>() {
            private ConfigurationView next;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                advance();

                return next != null;
            }

            private void advance() {
                if (next != null)
                    return;

                while (!iters.isEmpty() && !iters.peek().get2().hasNext())
                    iters.remove();

                if (!iters.isEmpty()) {
                    GridTuple3<Object, Iterator<Map.Entry<String, Method>>, String> curr = iters.peek();

                    try {
                        Map.Entry<String, Method> prop = curr.get2().next();

                        Object val = prop.getValue().invoke(curr.get1());

                        boolean res = addToQueue(
                            val,
                            (curr.get3().isEmpty() ? prop.getKey() : metricName(curr.get3(), prop.getKey()))
                        );

                        if (res)
                            advance();
                        else {
                            String valStr;

                            if (val != null && val.getClass().isArray())
                                valStr = S.arrayToString(val);
                            else
                                valStr = U.toStringSafe(val);

                            next = new ConfigurationView(
                                curr.get3().isEmpty() ? prop.getKey() : (curr.get3() + "." + prop.getKey()),
                                valStr
                            );
                        }
                    }
                    catch (IllegalAccessException | InvocationTargetException e) {
                        throw new IgniteException(e);
                    }
                }
            }

            /** {@inheritDoc} */
            @Override public ConfigurationView next() {
                if (next == null)
                    advance();

                ConfigurationView next0 = next;

                if (next0 == null)
                    throw new NoSuchElementException();

                next = null;

                return next0;
            }
        };
    }

    /** */
    private boolean addToQueue(Object val, String prefix) {
        if (val == null || val.getClass().isEnum())
            return false;

        Class<?> cls = val.getClass();

        boolean isArray = cls.isArray();

        if (isArray)
            cls = cls.getComponentType();

        if (!cls.getName().startsWith("org.apache.ignite"))
            return false;

        if (isArray) {
            int length = Array.getLength(val);

            if (length == 0)
                return false;

            for (int i = 0; i < length; i++) {
                Object el = Array.get(val, i);
                iters.add(F.t(el, props(el.getClass()), prefix + '[' + i + ']'));
            }
        }

        iters.add(F.t(val, props(val.getClass()), prefix));

        return true;
    }

    /**
     * @param cls Class to find properties.
     * @return Iterator of object fields.
     */
    private Iterator<Map.Entry<String, Method>> props(Class<?> cls) {
        Map<String, Method> props = new TreeMap<>(); // TreeMap to keep properties sorted

        for (; cls != Object.class; cls = cls.getSuperclass()) {
            for (Method mtd : cls.getMethods()) {
                if (mtd.getName().startsWith("set")) {
                    String propName = mtd.getName().substring(3);

                    Method getter = methodOrNull(cls, "get" + propName);

                    if (getter == null)
                        getter = methodOrNull(cls, "is" + propName);

                    if (getter != null && !props.containsKey(propName))
                        props.put(propName, getter);
                }
            }
        }

        return props.entrySet().iterator();
    }

    /**
     * @param cls Class to get method from.
     * @param name Name of the method.
     * @return Method if exists, {@code null} otherwise.
     */
    private static Method methodOrNull(Class<?> cls, String name) {
        try {
            return cls.getMethod(name);
        }
        catch (NoSuchMethodException ignore) {
            return null;
        }
    }
}
