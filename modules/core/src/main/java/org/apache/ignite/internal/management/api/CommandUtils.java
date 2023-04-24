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
import java.util.Collection;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteUuid;
import static org.apache.ignite.internal.management.api.AbstractCommand.CMD_NAME_POSTFIX;

/**
 * Utility class for management commands.
 */
public class CommandUtils {
    /** CLI named parameter prefix. */
    public static final String PARAMETER_PREFIX = "--";

    /** Delimeter for words in parameter and command names. */
    public static final char CMD_WORDS_DELIM = '-';

    /** Delimeter for words in positional parameters and parameter examples. */
    public static final char PARAM_WORDS_DELIM = '_';

    /** Indent for help output. */
    public static final String INDENT = "  ";

    /** Double indent for help output. */
    public static final String DOUBLE_INDENT = INDENT + INDENT;

    /**
     * Example: {@code "SystemView" -> "system-view"}.
     *
     * @param cls Command name class.
     * @param delim Words delimeter.
     * @return Formatted command name.
     */
    public static String toFormattedCommandName(Class<?> cls, char delim) {
        String name = cls.getSimpleName();

        assert name.endsWith(CMD_NAME_POSTFIX);

        return toFormattedName(name.substring(0, name.length() - CMD_NAME_POSTFIX.length()), delim);
    }

    /**
     * @param fld Field.
     * @return Formatted name of parameter for this field.
     */
    public static String toFormattedFieldName(Field fld) {
        return PARAMETER_PREFIX + toFormattedFieldName(fld, CMD_WORDS_DELIM);
    }

    /**
     * @param name Field, command name.
     * @param delim Words delimeter.
     * @return Formatted name.
     */
    public static String toFormattedName(String name, char delim) {
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

    /**
     * Example: {@code "system-view" -> "SystemView"}.
     *
     * @param formatted Formatted command name.
     * @param delim Words delimeter.
     * @return Source command name.
     */
    public static String fromFormattedCommandName(String formatted, char delim) {
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

    /**
     * @param fld Field.
     * @param appendOptional If {@code true} then example must be marked as optional.
     * @return Example of the field.
     */
    public static String parameterExample(Field fld, boolean appendOptional) {
        if (fld.isAnnotationPresent(Positional.class)) {
            Argument arg = fld.getAnnotation(Argument.class);

            return asOptional(
                arg.example().isEmpty()
                    ? toFormattedFieldName(fld, PARAM_WORDS_DELIM)
                    : arg.example(),
                appendOptional && arg.optional()
            );
        }

        Argument param = fld.getAnnotation(Argument.class);

        String example = valueExample(fld);

        return asOptional(
            toFormattedFieldName(fld) + (example.isEmpty() ? "" : (" " + example)),
            appendOptional && param.optional()
        );
    }

    /**
     * @param fld Field
     * @return Example of value for the field.
     */
    public static String valueExample(Field fld) {
        if (fld.getType() == Boolean.class || fld.getType() == boolean.class)
            return "";

        Argument param = fld.getAnnotation(Argument.class);

        if (!param.example().isEmpty())
            return param.example();

        boolean optional = fld.isAnnotationPresent(Positional.class) && param.optional();

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

        String name = toFormattedFieldName(fld, PARAM_WORDS_DELIM);

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

        return asOptional(name, optional);
    }

    /**
     * @param fld Field.
     * @param delim Words delimeter.
     * @return Name of the field.
     */
    private static String toFormattedFieldName(Field fld, char delim) {
        if (fld.isAnnotationPresent(Positional.class)) {
            return fld.getAnnotation(Argument.class).javaStyleExample()
                ? fld.getName()
                : toFormattedName(fld.getName(), delim);
        }

        return toFormattedName(fld.getName(), delim);
    }

    /** */
    static String asOptional(String str, boolean optional) {
        return (optional ? "[" : "") + str + (optional ? "]" : "");
    }

    /**
     * @param val String value.
     * @param type Class of the value.
     * @return Value.
     * @param <T> Value type.
     */
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

    /**
     * Parse and return single value (without support of array type).
     *
     * @param val String value.
     * @param type Class of the value.
     * @return Value.
     * @param <T> Value type
     */
    private static <T> T parseSingleVal(String val, Class<T> type) {
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
