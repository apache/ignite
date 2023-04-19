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

package org.apache.ignite.internal.management.api;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteUuid;

import static java.util.Collections.singleton;
import static org.apache.ignite.internal.management.api.BaseCommand.CMD_NAME_POSTFIX;

/**
 *
 */
public class CommandUtils {
    /** */
    public static final String PARAMETER_PREFIX = "--";

    /** */
    public static final char CMD_WORDS_DELIM = '-';

    /** */
    public static final char PARAM_WORDS_DELIM = '_';

    /** */
    public static String commandName(Class<?> cls, char delim) {
        String name = cls.getSimpleName();

        assert name.endsWith(CMD_NAME_POSTFIX);

        return formattedName(name.substring(0, name.length() - CMD_NAME_POSTFIX.length()), delim);
    }

    /** */
    public static <C extends IgniteDataTransferObject> C arguments(
        Class<C> argCls,
        BiFunction<Field, Integer, Object> positionalParameterProvider,
        Function<Field, Object> paramProvider
    ) throws InstantiationException, IllegalAccessException {
        C arg = argCls.newInstance();

        AtomicInteger position = new AtomicInteger();

        AtomicBoolean oneOfSet = new AtomicBoolean(false);

        Set<String> oneOfFields = arg.getClass().isAnnotationPresent(OneOf.class)
            ? new HashSet<>(Arrays.asList(arg.getClass().getAnnotation(OneOf.class).value()))
            : Collections.emptySet();

        BiConsumer<Field, Object> fldSetter = (fld, val) -> {
            if (val == null) {
                boolean optional = fld.isAnnotationPresent(Argument.class)
                    ? fld.getAnnotation(Argument.class).optional()
                    : fld.getAnnotation(PositionalArgument.class).optional();

                if (optional)
                    return;

                String name = fld.isAnnotationPresent(Argument.class)
                    ? parameterName(fld)
                    : parameterExample(fld, false);

                throw new IllegalArgumentException("Argument " + name + " required.");
            }

            if (oneOfFields.contains(fld.getName())) {
                if (oneOfSet.get())
                    throw new IllegalArgumentException("Only one of " + oneOfFields + " allowed");

                oneOfSet.set(true);
            }

            try {
                // TODO: use setters here.
                fld.setAccessible(true);
                fld.set(arg, val);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        };

        visitCommandParams(
            arg.getClass(),
            fld -> fldSetter.accept(fld, positionalParameterProvider.apply(fld, position.getAndIncrement())),
            fld -> fldSetter.accept(fld, paramProvider.apply(fld)),
            (optionals, flds) -> flds.forEach(fld -> fldSetter.accept(fld, paramProvider.apply(fld)))
        );

        boolean oneOfRequired = arg.getClass().isAnnotationPresent(OneOf.class)
            && !arg.getClass().getAnnotation(OneOf.class).optional();

        if (oneOfRequired && !oneOfSet.get())
            throw new IllegalArgumentException("One of " + oneOfFields + " required");

        return arg;
    }

    /** */
    public static String parameterExample(Field fld, boolean appendOptional) {
        if (fld.isAnnotationPresent(PositionalArgument.class)) {
            PositionalArgument posParam = fld.getAnnotation(PositionalArgument.class);

            return asOptional(
                posParam.example().isEmpty()
                    ? name(fld, PARAM_WORDS_DELIM, Argument::javaStyleName)
                    : posParam.example(),
                appendOptional && posParam.optional()
            );
        }

        Argument param = fld.getAnnotation(Argument.class);

        String example = valueExample(fld);

        return asOptional(
            parameterName(fld) + (example.isEmpty() ? "" : (" " + example)),
            appendOptional && param.optional()
        );
    }

    /** */
    public static String valueExample(Field fld) {
        if (fld.getType() == Boolean.class || fld.getType() == boolean.class)
            return "";

        PositionalArgument posParam = fld.getAnnotation(PositionalArgument.class);
        Argument param = fld.getAnnotation(Argument.class);

        if (posParam != null) {
            String example = posParam.example();

            if (!example.isEmpty())
                return example;
        }
        else if (!param.example().isEmpty())
            return param.example();

        boolean optional = posParam != null && posParam.optional();

        if (Enum.class.isAssignableFrom(fld.getType())) {
            Object[] vals = fld.getType().getEnumConstants();

            StringBuilder bldr = new StringBuilder();

            for (int i = 0; i < vals.length; i++) {
                if (i != 0)
                    bldr.append('|');

                bldr.append(((Enum<?>)vals[i]).name());
            }

            return asOptional(bldr.toString(), optional);
        }

        boolean brackets = param != null && param.brackets();

        String name = name(fld, PARAM_WORDS_DELIM, Argument::javaStyleExample);

        if (fld.getType().isArray() || Collection.class.isAssignableFrom(fld.getType())) {
            if (name.endsWith("s"))
                name = name.substring(0, name.length() - 1);

            char last = name.charAt(name.length() - 1);

            if (Character.isUpperCase(last)) {
                name = name.substring(0, name.length() - 1) + Character.toLowerCase(last);
            }

            String example = name + "1[," + name + "2,....," + name + "N]";

            return asOptional(example, optional);
        }

        if (brackets)
            name = '<' + name + '>';

        return asOptional(name, optional);
    }

    /**
     * Iterates and visits each command parameter.
     */
    public static <C extends IgniteDataTransferObject> void visitCommandParams(
        Class<C> clazz,
        Consumer<Field> positionalParamVisitor,
        Consumer<Field> namedParamVisitor,
        BiConsumer<Boolean, List<Field>> oneOfNamedParamVisitor
    ) {
        List<Field> positionalParams = new ArrayList<>();
        List<Field> namedParams = new ArrayList<>();

        OneOf oneOf = clazz.getAnnotation(OneOf.class);

        Set<String> oneOfNames = oneOf != null
            ? new HashSet<>(Arrays.asList(oneOf.value()))
            : Collections.emptySet();

        List<Field> oneOfFlds = new ArrayList<>();

        Class<? extends IgniteDataTransferObject> clazz0 = clazz;

        while (clazz0 != IgniteDataTransferObject.class) {
            Field[] flds = clazz0.getDeclaredFields();

            for (Field fld : flds) {
                if (oneOfNames.contains(fld.getName()))
                    oneOfFlds.add(fld);
                else if (fld.isAnnotationPresent(PositionalArgument.class))
                    positionalParams.add(fld);
                else if (fld.isAnnotationPresent(Argument.class))
                    namedParams.add(fld);
            }

            if (IgniteDataTransferObject.class.isAssignableFrom(clazz0.getSuperclass()))
                clazz0 = (Class<? extends IgniteDataTransferObject>)clazz0.getSuperclass();
            else
                break;
        }

        positionalParams.forEach(positionalParamVisitor);

        namedParams.forEach(namedParamVisitor);

        if (oneOf != null)
            oneOfNamedParamVisitor.accept(oneOf.optional(), oneOfFlds);
    }

    /** */
    private static String name(Field fld, char delim, Predicate<Argument> javaStyle) {
        if (fld.isAnnotationPresent(PositionalArgument.class)) {
            PositionalArgument posParam = fld.getAnnotation(PositionalArgument.class);

            return posParam.javaStyleExample() ? fld.getName() : formattedName(fld.getName(), delim);
        }

        return javaStyle.test(fld.getAnnotation(Argument.class))
            ? fld.getName()
            : formattedName(fld.getName(), delim);
    }

    /** */
    private static String asOptional(String str, boolean optional) {
        return (optional ? "[" : "") + str + (optional ? "]" : "");
    }

    /**
     * @param name Field, command name.
     * @param delim Words delimeter.
     * @return Formatted name.
     */
    public static String formattedName(String name, char delim) {
        StringBuilder formatted = new StringBuilder();

        formatted.append(Character.toLowerCase(name.charAt(0)));

        int i = 1;

        while (i < name.length()) {
            if (Character.isLowerCase(name.charAt(i))) {
                formatted.append(name.charAt(i));

                i++;
            }
            else {
                formatted.append(delim);
                formatted.append(Character.toLowerCase(name.charAt(i)));
                i++;
            }
        }

        return formatted.toString();
    }

    /** */
    public static String fromFormattedName(String formatted, char delim) {
        StringBuilder name = new StringBuilder();

        name.append(Character.toUpperCase(formatted.charAt(0)));

        int i = 1;

        while (i < formatted.length()) {
            if (formatted.charAt(i) != delim) {
                name.append(formatted.charAt(i));

                i++;
            }
            else {
                i++;
                name.append(Character.toUpperCase(formatted.charAt(i)));
                i++;
            }
        }

        return name.toString();
    }

    /** */
    public static boolean hasDescribedParameters(Command<?, ?, ?> cmd) {
        AtomicBoolean res = new AtomicBoolean();

        visitCommandParams(
            cmd.args(),
            fld -> res.compareAndSet(false,
                !fld.getAnnotation(PositionalArgument.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ),
            fld -> res.compareAndSet(
                false,
                !fld.getAnnotation(Argument.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ),
            (spaceReq, flds) -> flds.forEach(fld -> res.compareAndSet(
                false,
                !(fld.isAnnotationPresent(Argument.class)
                    ? fld.getAnnotation(Argument.class).description()
                    : fld.getAnnotation(PositionalArgument.class).description()
                ).isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ))
        );

        return res.get();
    }

    /** */
    public static String parameterName(Field fld) {
        return (fld.getAnnotation(Argument.class).withoutPrefix() ? "" : PARAMETER_PREFIX)
            + name(fld, CMD_WORDS_DELIM, Argument::javaStyleName);
    }

    /** */
    public static boolean isArgumentName(String arg) {
        return arg.startsWith(PARAMETER_PREFIX);
    }

    /** */
    public static <T> T parseVal(String val, Class<T> type) {
        if (type.isArray()) {
            String[] vals = val.split(",");

            Class<?> compType = type.getComponentType();

            if (compType == String.class)
                return (T)vals;

            Object[] res = (Object[])Array.newInstance(compType, vals.length);

            for (int i = 0; i < vals.length; i++)
                res[i] = parseSingleVal(vals[i], compType);

            return (T)res;
        }

        return parseSingleVal(val, type);
    }

    /** */
    public static <T> T parseSingleVal(String val, Class<T> type) {
        if (type == Boolean.class || type == boolean.class)
            return (T)Boolean.TRUE;
        if (type == String.class)
            return (T)val;
        else if (type == Integer.class || type == int.class)
            return (T)wrapNumberFormatException(() -> Integer.parseInt(val), val, Integer.class);
        else if (type == Long.class || type == long.class)
            return (T)wrapNumberFormatException(() -> Long.parseLong(val), val, Long.class);
        else if (type == UUID.class) {
            try {
                return (T)UUID.fromString(val);
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("String representation of \"java.util.UUID\" is exepected. " +
                    "For example: 123e4567-e89b-42d3-a456-556642440000");
            }
        }
        else if (type == IgniteUuid.class) {
            return (T)IgniteUuid.fromString(val);
        }

        throw new IgniteException("Unsupported argument type: " + type.getName());
    }

    /** */
    public static <A extends IgniteDataTransferObject> void executeAndPrint(
        IgniteEx grid,
        Command<A, ?, ?> cmd,
        A arg,
        Consumer<String> printer
    ) {
        IgniteCompute compute = grid.compute();

        Map<UUID, ClusterNode> clusterNodes = grid.cluster().nodes().stream()
            .collect(Collectors.toMap(ClusterNode::id, n -> n));

        Collection<UUID> nodeIds = cmd.nodes(clusterNodes.keySet(), arg);

        for (UUID id : nodeIds) {
            if (!clusterNodes.containsKey(id))
                throw new IllegalArgumentException("Node with id=" + id + " not found.");
        }

        if (nodeIds.isEmpty())
            nodeIds = singleton(grid.localNode().id());

        if (!F.isEmpty(nodeIds))
            compute = grid.compute(grid.cluster().forNodeIds(nodeIds));

        Object res = compute.execute(cmd.task().getName(), new VisorTaskArgument<>(nodeIds, arg, false));

        cmd.printResult(arg, res, printer);
    }

    /**
     * Wrap {@link NumberFormatException} to get more user friendly message.
     *
     * @param closure Closure that parses number.
     * @param val String value.
     * @param expectedType Expected type.
     * @return Parsed result, if parse had success.
     */
    private static Object wrapNumberFormatException(Supplier<Object> closure, String val, Class<? extends Number> expectedType) {
        try {
            return closure.get();
        }
        catch (NumberFormatException e) {
            throw new NumberFormatException("Can't parse number '" + val + "', expected type: " + expectedType.getName());
        }
    }
}
