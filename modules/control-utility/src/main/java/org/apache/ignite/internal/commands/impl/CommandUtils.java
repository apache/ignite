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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.internal.commands.api.Command;
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
    public static final char CMD_WORDS_DELIM = '-';

    /** */
    public static final char PARAM_WORDS_DELIM = '_';

    /**
     * Turn java style name to command name.
     * For example:
     * "MyCommand" -> "My-command"
     * "nodeIDs" -> "node-ids"
     * "myLongName" -> "my-long-name
     *
     * @return Command name.
     */
    public static String commandName(String name) {
        return formattedName(name, CMD_WORDS_DELIM);
    }

    /** */
    public static String commandName(Class<? extends Command> cls) {
        String name = cls.getSimpleName();

        return commandName(name.substring(0, name.length() - CMD_NAME_POSTFIX.length()));
    }

    /**
     * For example:
     * "MyCommand" -> "my_command"
     * "nodeIDs" -> "node_ids"
     * "myLongName" -> "my_long_name
     *
     * @return Positional parameter name.
     */
    public static String parameterName(String name) {
        return formattedName(name, PARAM_WORDS_DELIM);
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
    public static String parameterName(Field fld) {
        Parameter desc = fld.getAnnotation(Parameter.class);

        return desc == null || !desc.javaStyleName()
            ? commandName(fld.getName())
            : fld.getName();
    }

    /** */
    public static String examples(Field fld) {
        if (fld.getType() == Boolean.class || fld.getType() == boolean.class)
            return "";

        if (fld.isAnnotationPresent(PositionalParameter.class)) {
            String example = fld.getAnnotation(PositionalParameter.class).example();

            if (!example.isEmpty())
                return example;
        }
        else {
            String example = fld.getAnnotation(Parameter.class).example();

            if (!example.isEmpty())
                return example;
        }

        boolean optional = (fld.isAnnotationPresent(PositionalParameter.class)
            && fld.getAnnotation(PositionalParameter.class).optional());

        if (Enum.class.isAssignableFrom(fld.getType())) {
            Object[] vals = fld.getType().getEnumConstants();

            StringBuilder bldr = new StringBuilder();

            for (int i = 0; i < vals.length; i++) {
                if (i != 0)
                    bldr.append('|');

                bldr.append(((Enum<?>)vals[i]).name());
            }

            return toOptional(bldr.toString(), optional);
        }

        boolean javaStyleExample = (fld.isAnnotationPresent(Parameter.class)
                    && fld.getAnnotation(Parameter.class).javaStyleExample())
            || (fld.isAnnotationPresent(PositionalParameter.class)
                    && fld.getAnnotation(PositionalParameter.class).javaStyleExample());

        boolean brackets = (fld.isAnnotationPresent(Parameter.class) && fld.getAnnotation(Parameter.class).brackets());

        String name = javaStyleExample ? fld.getName() : parameterName(fld.getName());

        if (fld.getType().isArray() || Collection.class.isAssignableFrom(fld.getType())) {
            if (name.endsWith("s"))
                name = name.substring(0, name.length() - 1);

            char last = name.charAt(name.length() - 1);

            if (Character.isUpperCase(last)) {
                name = name.substring(0, name.length() - 1) + Character.toLowerCase(last);
            }

            String example = name + "1[," + name + "2,....," + name + "N]";

            return toOptional(example, optional);
        }

        if (brackets)
            name = '<' + name + '>';

        return toOptional(name, optional);
    }

    /** */
    private static String toOptional(String str, boolean optional) {
        return (optional ? "[" : "") + str + (optional ? "]" : "");
    }

    /**
     * Iterates each field of the {@code clazz} and call consume {@code c} for it.
     *
     * @param posCnsmr Positional parameter fields consumer.
     * @param namedCnsmr Named parameter fields consumer.
     */
    public static void forEachField(
        Class<? extends Command> clazz,
        Consumer<Field> posCnsmr,
        Consumer<Field> namedCnsmr,
        BiConsumer<Boolean, List<Field>> oneOfCnsmr
    ) {
        while (true) {
            Field[] flds = clazz.getDeclaredFields();

            List<Field> posParams = new ArrayList<>();
            List<Field> namedParams = new ArrayList<>();

            OneOf oneOf = clazz.getAnnotation(OneOf.class);

            Set<String> oneOfNames = oneOf != null
                ? new HashSet<>(Arrays.asList(oneOf.value()))
                : Collections.emptySet();

            List<Field> oneOfFlds = new ArrayList<>();

            for (Field fld : flds) {
                if (oneOfNames.contains(fld.getName()))
                    oneOfFlds.add(fld);
                else if (fld.isAnnotationPresent(PositionalParameter.class))
                    posParams.add(fld);
                else if (fld.isAnnotationPresent(Parameter.class))
                    namedParams.add(fld);
            }

            posParams.forEach(posCnsmr);
            namedParams.forEach(namedCnsmr);
            if (oneOf != null)
                oneOfCnsmr.accept(oneOf.optional(), oneOfFlds);

            if (Command.class.isAssignableFrom(clazz.getSuperclass()))
                clazz = (Class<? extends Command>)clazz.getSuperclass();
            else
                break;
        }
    }
}
