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
import java.util.function.BiFunction;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.systemview.SensitiveProperty;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.ConfigurationView;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIGURATION_VIEW_PACKAGES;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.IgniteUtils.IGNITE_PKG;

/**
 * Responsibility of this class it to recursively iterate {@link IgniteConfiguration} object
 * and expose all properties in form of String pairs.
 * Properties of configuration classes are collected via reflection once, the configuration is iterated on each iteration.
 * Properties marked as {@link SensitiveProperty} and {@code char[]} properties (usually passwords) are never exposed.
 * Only objects of classes from the configured packages are iterated, for an array this is checked for each element.
 * Cyclic references are not iterated: such a property is exposed as a plain value, such an array element is skipped.
 * Array elements that are {@code null} or not iterated by package are skipped as well.
 * An array without elements to iterate is exposed as a plain value.
 */
public class IgniteConfigurationIterable implements Iterable<ConfigurationView> {
    /** Properties of configuration classes: names and getters. */
    private static final ClassValue<Map<String, Method>> PROPS = new ClassValue<>() {
        @Override protected Map<String, Method> computeValue(Class<?> cls) {
            return props(cls);
        }
    };

    /** Packages to expose objects from. */
    private final List<String> pkgs;

    /** Configuration to iterate. */
    private final IgniteConfiguration cfg;

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

        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public Iterator<ConfigurationView> iterator() {
        return iterator0((name, val) -> new ConfigurationView(
            name,
            val != null && val.getClass().isArray()
                ? S.arrayToString(val)
                : U.toStringSafe(val)
        ));
    }

    /**
     * @param trans Transformer of a property full name and raw value to an iteration element.
     * @param <T> Type of iteration elements.
     * @return Iterator of configuration properties transformed by {@code trans}.
     */
    protected <T> Iterator<T> iterator0(BiFunction<String, Object, T> trans) {
        Queue<Node> iters = new LinkedList<>();

        iters.add(new Node(cfg, objectProperties(cfg), "", null));

        return new Iterator<T>() {
            private T next;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                while (next == null && !iters.isEmpty())
                    advance();

                return next != null;
            }

            private void advance() {
                if (next != null)
                    return;

                while (!iters.isEmpty() && !iters.peek().props.hasNext())
                    iters.remove();

                if (!iters.isEmpty()) {
                    Node curr = iters.peek();

                    try {
                        Map.Entry<String, Method> prop = curr.props.next();

                        String name = curr.prefix.isEmpty() ? prop.getKey() : metricName(curr.prefix, prop.getKey());

                        Object val = prop.getValue().invoke(curr.obj);

                        if (addToQueue(iters, val, name, curr))
                            return;

                        next = trans.apply(name, val);
                    }
                    catch (IllegalAccessException | InvocationTargetException e) {
                        throw new IgniteException(e);
                    }
                }
            }

            /** {@inheritDoc} */
            @Override public T next() {
                while (next == null && !iters.isEmpty())
                    advance();

                T next0 = next;

                if (next0 == null)
                    throw new NoSuchElementException();

                next = null;

                return next0;
            }
        };
    }

    /**
     * If {@code val} is configuration bean that must be recursively exposed by the view then
     * it will be added to the {@code iters} queue.
     *
     * @param iters Queue of objects to iterate.
     * @param val Value.
     * @param prefix Name prefix of the value properties.
     * @param parent Queue entry of the object the value belongs to.
     * @return {@code True} if {@code val} was added to {@code iters} queue, {@code false} otherwise.
     */
    private boolean addToQueue(Queue<Node> iters, Object val, String prefix, Node parent) {
        if (val == null || val.getClass().isEnum())
            return false;

        Class<?> cls = val.getClass();

        if (cls.isArray()) {
            if (cls.getComponentType().isPrimitive())
                return false;

            int length = Array.getLength(val);

            boolean added = false;

            for (int i = 0; i < length; i++) {
                Object el = Array.get(val, i);

                if (checkObjectPackage(el) && !Node.contains(parent, el)) {
                    iters.add(new Node(el, objectProperties(el), prefix + '[' + i + ']', parent));

                    added = true;
                }
            }

            return added;
        }

        if (!checkPkg(cls.getName()) || Node.contains(parent, val))
            return false;

        iters.add(new Node(val, objectProperties(val), prefix, parent));

        return true;
    }

    /**
     * Checks package of an array element.
     *
     * @param val Array element.
     * @return {@code True} if {@code val} is a configuration bean that must be recursively exposed by the view.
     */
    private boolean checkObjectPackage(@Nullable Object val) {
        return val != null && !val.getClass().isEnum() && checkPkg(val.getClass().getName());
    }

    /**
     * @param obj Object.
     * @return Iterator of object properties: names and getters.
     */
    private Iterator<Map.Entry<String, Method>> objectProperties(Object obj) {
        return PROPS.get(obj.getClass()).entrySet().iterator();
    }

    /**
     * Sensitive properties are skipped.
     *
     * @param cls Class to find properties.
     * @return Unmodifiable object properties sorted by name: names and getters.
     */
    private static Map<String, Method> props(Class<?> cls) {
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

        props.values().removeIf(IgniteConfigurationIterable::isSensitive);

        return Collections.unmodifiableMap(props);
    }

    /**
     * Getter can be overridden in a subclass, so the superclass chain is checked (interfaces are not checked).
     *
     * @param getter Property getter.
     * @return {@code True} if property is marked as {@link SensitiveProperty} or has {@code char[]} type.
     */
    private static boolean isSensitive(Method getter) {
        if (getter.getReturnType() == char[].class)
            return true;

        for (Class<?> cls = getter.getDeclaringClass(); cls != null; cls = cls.getSuperclass()) {
            Method mtd = methodOrNull(cls, getter.getName());

            if (mtd != null && mtd.isAnnotationPresent(SensitiveProperty.class))
                return true;
        }

        return false;
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

    /** Queue entry: object to iterate. */
    private static class Node {
        /** Object. */
        private final Object obj;

        /** Iterator of object properties. */
        private final Iterator<Map.Entry<String, Method>> props;

        /** Name prefix of object properties. */
        private final String prefix;

        /** Entry of the object this object belongs to, {@code null} for the root. */
        @Nullable private final Node parent;

        /**
         * @param obj Object.
         * @param props Iterator of object properties.
         * @param prefix Name prefix of object properties.
         * @param parent Entry of the object this object belongs to, {@code null} for the root.
         */
        private Node(Object obj, Iterator<Map.Entry<String, Method>> props, String prefix, @Nullable Node parent) {
            this.obj = obj;
            this.props = props;
            this.prefix = prefix;
            this.parent = parent;
        }

        /**
         * @param node Entry to start from.
         * @param obj Object.
         * @return {@code True} if {@code obj} is the object of {@code node} or of any of its parents.
         */
        private static boolean contains(@Nullable Node node, Object obj) {
            for (; node != null; node = node.parent) {
                if (node.obj == obj)
                    return true;
            }

            return false;
        }
    }
}
