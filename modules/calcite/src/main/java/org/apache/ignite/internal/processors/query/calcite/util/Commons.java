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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods.
 */
public final class Commons {
    /** */
    private Commons(){}

    /**
     * Converts a QueryContext into a planner context.
     * @param ctx QueryContext.
     * @return Planner context.
     */
    public static Context convert(QueryContext ctx) {
        return ctx == null ? Contexts.empty() : Contexts.of(ctx.unwrap(Object[].class));
    }

    /**
     * Intersects two lists.
     */
    public static <T> List<T> intersect(List<T> left, List<T> right) {
        if (F.isEmpty(left) || F.isEmpty(right))
            return Collections.emptyList();

        return intersect0(left, right);
    }

    /**
     * Intersects a set and a list.
     *
     * @return A List of unique entries that presented in both the given set and the given list.
     */
    public static <T> List<T> intersect(Set<T> set, List<T> list) {
        if (F.isEmpty(set) || F.isEmpty(list))
            return Collections.emptyList();

        List<T> res = new ArrayList<>(Math.min(set.size(), list.size()));

        for (T t : list) {
            if (set.contains(t))
                res.add(t);
        }

        return res;
    }

    /** */
    private static <T> List<T> intersect0(@NotNull List<T> left, @NotNull List<T> right) {
        if (left.size() > right.size())
            return intersect0(right, left);

        return intersect(new HashSet<>(left), right);
    }

    /**
     * Returns a given list as a typed list.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> List<T> cast(List<?> src) {
        return (List)src;
    }

    /**
     * Transforms a given list using map function.
     */
    public static <T,R> List<R> transform(@NotNull List<T> src, @NotNull Function<T,R> mapFun) {
        if (F.isEmpty(src))
            return Collections.emptyList();

        List<R> list = new ArrayList<>(src.size());

        for (T t : src)
            list.add(mapFun.apply(t));

        return list;
    }

    /**
     * Extracts planner context.
     */
    public static PlanningContext context(RelNode rel) {
        return context(rel.getCluster());
    }

    /**
     * Extracts planner context.
     */
    public static PlanningContext context(RelOptCluster cluster) {
        return context(cluster.getPlanner().getContext());
    }

    /**
     * Extracts planner context.
     */
    public static PlanningContext context(Context ctx) {
        return Objects.requireNonNull(ctx.unwrap(PlanningContext.class));
    }

    /**
     * @param params Parameters.
     * @return Parameters map.
     */
    public static Map<String, Object> parametersMap(@Nullable Object[] params) {
        HashMap<String, Object> res = new HashMap<>();

        return params != null ? populateParameters(res, params) : res;
    }

    /**
     * Populates a provided map with given parameters.
     *
     * @param dst Map to populate.
     * @param params Parameters.
     * @return Parameters map.
     */
    public static Map<String, Object> populateParameters(@NotNull Map<String, Object> dst, @Nullable Object[] params) {
        if (!F.isEmpty(params)) {
            for (int i = 0; i < params.length; i++) {
                dst.put("?" + i, params[i]);
            }
        }
        return dst;
    }

    /**
     * Lookups a specific component in registered components list.
     *
     * @param ctx Kernal context.
     * @param componentType Component type.
     * @return Component instance or {@code null} if not found.
     */
    public static <T extends GridComponent> T lookupComponent(GridKernalContext ctx, Class<T> componentType) {
        return ctx.components().stream()
            .filter(componentType::isInstance)
            .map(componentType::cast)
            .findFirst().orElse(null);
    }

    /**
     * @param o Object to close.
     */
    public static void close(Object o) throws Exception {
        if (o instanceof AutoCloseable)
            ((AutoCloseable) o).close();
    }

    /**
     * @param o Object to close.
     */
    public static void close(Object o, IgniteLogger log) {
        if (o instanceof AutoCloseable)
            U.close((AutoCloseable) o, log);
    }

    /**
     * @param o Object to close.
     */
    public static void closeQuiet(Object o) {
        if (o instanceof AutoCloseable)
            U.closeQuiet((AutoCloseable) o);
    }

    /**
     * @param o Object to close.
     * @param e Exception, what causes close.
     */
    public static void closeQuiet(Object o, @Nullable Exception e) {
        if (!(o instanceof AutoCloseable))
            return;

        if (e != null)
            U.closeWithSuppressingException((AutoCloseable) o, e);
        else
            U.closeQuiet((AutoCloseable) o);
    }

    /** */
    public static <T> List<T> flat(List<List<? extends T>> src) {
        return src.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    /** */
    public static int max(ImmutableIntList list) {
        if (list.isEmpty())
            throw new UnsupportedOperationException();

        int res = list.getInt(0);

        for (int i = 1; i < list.size(); i++)
            res = Math.max(res, list.getInt(i));

        return res;
    }

    /** */
    public static int min(ImmutableIntList list) {
        if (list.isEmpty())
            throw new UnsupportedOperationException();

        int res = list.getInt(0);

        for (int i = 1; i < list.size(); i++)
            res = Math.min(res, list.getInt(i));

        return res;
    }

    /** */
    public static <T> T compile(Class<T> interfaceType, String body) {
        final boolean debug = CalciteSystemProperty.DEBUG.value();

        if (debug)
            Util.debugCode(System.out, body);

        try {
            final ICompilerFactory compilerFactory;

            try {
                compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Unable to instantiate java compiler", e);
            }

            IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();

            cbe.setImplementedInterfaces(new Class[]{ interfaceType });
            cbe.setParentClassLoader(ExpressionFactoryImpl.class.getClassLoader());

            if (debug)
                // Add line numbers to the generated janino class
                cbe.setDebuggingInformation(true, true, true);

            return (T) cbe.createInstance(new StringReader(body));
        } catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public static void checkRange(@NotNull Object[] array, int idx) {
        if (idx < 0 || idx >= array.length)
            throw new ArrayIndexOutOfBoundsException(idx);
    }

    /** */
    public static <T> T[] ensureCapacity(T[] array, int required) {
        A.ensure(required >= 0, "capacity must not be negative");

        return array.length <= required ? Arrays.copyOf(array, U.nextPowerOf2(required)) : array;
    }
}
