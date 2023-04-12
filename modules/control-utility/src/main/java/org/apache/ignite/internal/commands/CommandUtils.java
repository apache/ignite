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

package org.apache.ignite.internal.commands;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.OneOf;
import org.apache.ignite.internal.management.api.PositionalArgument;
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
        StringBuilder bld = new StringBuilder();

        bld.append(Character.toLowerCase(name.charAt(0)));

        int i = 1;

        while (i < name.length()) {
            if (Character.isLowerCase(name.charAt(i))) {
                bld.append(name.charAt(i));

                i++;
            }
            else {
                bld.append(delim);

                while (i < name.length() && Character.isUpperCase(name.charAt(i))) {
                    bld.append(Character.toLowerCase(name.charAt(i)));

                    i++;
                }
            }
        }

        return bld.toString();
    }

    /** */
    public static boolean hasDescribedParameters(Command<?> cmd) {
        AtomicBoolean res = new AtomicBoolean();

        visitCommandParams(
            cmd.args(),
            fld -> res.compareAndSet(false,
                !fld.getAnnotation(PositionalArgument.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ),
            fld -> res.compareAndSet(
                false,
                (!fld.getAnnotation(Argument.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class))
                    && !fld.getAnnotation(Argument.class).excludeFromDescription()

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
}
