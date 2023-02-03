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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.TreeMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.ConfigurationView;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIGURATION_VIEW_PACKAGES;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.IgniteUtils.IGNITE_PKG;

/**
 * Responsibility of this class it to recursively iterate {@link IgniteConfiguration} object
 * and expose all properties in form of String pairs.
 */
public class IgniteConfigurationIterable implements Iterable<ConfigurationView> {
    /** Packages to expose objects from. */
    private final List<String> pkgs;

    /** Iterators queue. */
    private final Queue<GridTuple3<Object, Iterator<Map.Entry<String, Method>>, String>> iters = new LinkedList<>();

    /**
     * @param cfg Configuration to iterate.
     */
    public IgniteConfigurationIterable(IgniteConfiguration cfg) {
        String pkgsProp = IgniteSystemProperties.getString(IGNITE_CONFIGURATION_VIEW_PACKAGES);

        pkgs = new ArrayList<>(F.isEmpty(pkgsProp)
            ? Collections.emptyList()
            : Arrays.asList(pkgsProp.split(","))
        );

        pkgs.add(IGNITE_PKG);

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

                        String name = curr.get3().isEmpty() ? prop.getKey() : metricName(curr.get3(), prop.getKey());

                        Object val = prop.getValue().invoke(curr.get1());

                        if (addToQueue(val, name)) {
                            advance();

                            return;
                        }

                        next = new ConfigurationView(
                            name,
                            val != null && val.getClass().isArray()
                                ? S.arrayToString(val)
                                : U.toStringSafe(val)
                        );
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

    /**
     * If {@code val} is configuration bean that must be recursively exposed by the view then
     * it will be added to the {@link #iters} queue.
     *
     * @return {@code True} if {@code val} was added to {@link #iters} queue, {@code false} otherwise.
     */
    private boolean addToQueue(Object val, String prefix) {
        if (val == null || val.getClass().isEnum())
            return false;

        Class<?> cls = val.getClass();

        boolean isArray = cls.isArray();

        if (isArray)
            cls = cls.getComponentType();

        if (!checkPkg(cls.getName()))
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
     * @return Iterator of object properties.
     */
    private Iterator<Map.Entry<String, Method>> props(Class<?> cls) {
        Map<String, Method> props = new TreeMap<>(); // TreeMap to keep properties sorted.

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

    /** */
    private boolean checkPkg(String name) {
        for (String pkg : pkgs) {
            if (name.startsWith(pkg))
                return true;
        }

        return false;
    }
}
