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

package org.apache.ignite.internal.commands.impl;

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
import org.apache.ignite.internal.commands.api.Command;
import org.apache.ignite.internal.commands.api.EnumDescription;
import org.apache.ignite.internal.commands.api.OneOf;
import org.apache.ignite.internal.commands.api.Parameter;
import org.apache.ignite.internal.commands.api.PositionalParameter;

/**
 *
 */
public class CommandUtils {
    /** */
    public static final String CMD_NAME_POSTFIX = "Command";

    /** */
    public static final String PARAMETER_PREFIX = "--";

    /** */
    public static final char CMD_WORDS_DELIM = '-';

    /** */
    public static final char PARAM_WORDS_DELIM = '_';

    /** */
    public static String commandName(Class<? extends Command> cls, char delim) {
        String name = cls.getSimpleName();

        return formattedName(name.substring(0, name.length() - CMD_NAME_POSTFIX.length()), delim);
    }

    /** */
    public static String parameterExample(Field fld, boolean appendOptional) {
        if (fld.isAnnotationPresent(PositionalParameter.class)) {
            PositionalParameter posParam = fld.getAnnotation(PositionalParameter.class);

            return asOptional(
                posParam.example().isEmpty()
                    ? name(fld, PARAM_WORDS_DELIM, Parameter::javaStyleName)
                    : posParam.example(),
                appendOptional && posParam.optional()
            );
        }

        Parameter param = fld.getAnnotation(Parameter.class);

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

        PositionalParameter posParam = fld.getAnnotation(PositionalParameter.class);
        Parameter param = fld.getAnnotation(Parameter.class);

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

        String name = name(fld, PARAM_WORDS_DELIM, Parameter::javaStyleExample);

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
    public static void visitCommandParams(
        Class<? extends Command> clazz,
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

        while (true) {
            Field[] flds = clazz.getDeclaredFields();

            for (Field fld : flds) {
                if (oneOfNames.contains(fld.getName()))
                    oneOfFlds.add(fld);
                else if (fld.isAnnotationPresent(PositionalParameter.class))
                    positionalParams.add(fld);
                else if (fld.isAnnotationPresent(Parameter.class))
                    namedParams.add(fld);
            }

            if (Command.class.isAssignableFrom(clazz.getSuperclass()))
                clazz = (Class<? extends Command>)clazz.getSuperclass();
            else
                break;
        }

        positionalParams.forEach(positionalParamVisitor);

        namedParams.forEach(namedParamVisitor);

        if (oneOf != null)
            oneOfNamedParamVisitor.accept(oneOf.optional(), oneOfFlds);
    }

    /** */
    private static String name(Field fld, char delim, Predicate<Parameter> javaStyle) {
        if (fld.isAnnotationPresent(PositionalParameter.class)) {
            PositionalParameter posParam = fld.getAnnotation(PositionalParameter.class);

            return posParam.javaStyleExample() ? fld.getName() : formattedName(fld.getName(), delim);
        }

        return javaStyle.test(fld.getAnnotation(Parameter.class))
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
    public static boolean hasDescribedParameters(Command cmd) {
        AtomicBoolean res = new AtomicBoolean();

        visitCommandParams(
            cmd.getClass(),
            fld -> res.compareAndSet(false,
                !fld.getAnnotation(PositionalParameter.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ),
            fld -> res.compareAndSet(
                false,
                (!fld.getAnnotation(Parameter.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class))
                    && !fld.getAnnotation(Parameter.class).excludeFromDescription()

            ),
            (spaceReq, flds) -> flds.forEach(fld -> res.compareAndSet(
                false,
                !(fld.isAnnotationPresent(Parameter.class)
                    ? fld.getAnnotation(Parameter.class).description()
                    : fld.getAnnotation(PositionalParameter.class).description()
                ).isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ))
        );

        return res.get();
    }

    /** */
    public static String parameterName(Field fld) {
        return (fld.getAnnotation(Parameter.class).withoutPrefix() ? "" : PARAMETER_PREFIX)
            + name(fld, CMD_WORDS_DELIM, Parameter::javaStyleName);
    }

    /** */
    public static boolean isArgumentName(String arg) {
        return arg.startsWith(PARAMETER_PREFIX);
    }
}
