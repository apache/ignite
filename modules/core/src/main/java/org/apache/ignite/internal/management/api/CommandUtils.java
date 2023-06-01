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
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.internal.management.api.Command.CMD_NAME_POSTFIX;

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
     * @return Formatted command name.
     */
    public static String toFormattedCommandName(Class<?> cls) {
        return toFormattedCommandName(cls, CMD_WORDS_DELIM);
    }

    /**
     * Example: {@code "SystemView" -> "system-view"}.
     *
     * @param cls Command name class.
     * @return Formatted command name.
     */
    public static String toFormattedCommandName(Class<?> cls, char delim) {
        String name = cls.getSimpleName();

        return toFormattedName(name.substring(0, name.length() - CMD_NAME_POSTFIX.length()), delim);
    }

    /**
     * @param fld Field.
     * @return Formatted name of parameter for this field.
     */
    public static String toFormattedFieldName(Field fld) {
        return (fld.getAnnotation(Argument.class).withoutPrefix() ? "" : PARAMETER_PREFIX)
            + toFormattedFieldName(fld, CMD_WORDS_DELIM);
    }

    /**
     * @param flds Fields to format.
     * @return Formatted names.
     */
    public static Set<String> toFormattedNames(Set<String> flds) {
        return flds.stream()
            .map(fld -> PARAMETER_PREFIX + toFormattedName(fld, CMD_WORDS_DELIM))
            .collect(Collectors.toSet());
    }

    /**
     * @param name Field, command name.
     * @param delim Words delimeter.
     * @return Formatted name.
     */
    static String toFormattedName(String name, char delim) {
        StringBuilder formatted = new StringBuilder();

        formatted.append(Character.toLowerCase(name.charAt(0)));

        int i = 1;

        while (i < name.length()) {
            if (Character.isLowerCase(name.charAt(i)))
                formatted.append(name.charAt(i));
            else {
                formatted.append(delim);
                formatted.append(Character.toLowerCase(name.charAt(i)));
            }

            i++;
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
            if (formatted.charAt(i) != delim)
                name.append(Character.toLowerCase(formatted.charAt(i)));
            else {
                i++;
                name.append(Character.toUpperCase(formatted.charAt(i)));
            }

            i++;
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

        boolean optional = fld.isAnnotationPresent(Positional.class) && param.optional();

        if (!param.example().isEmpty())
            return asOptional(param.example(), optional);

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

        return fld.getAnnotation(Argument.class).javaStyleName()
            ? fld.getName()
            : toFormattedName(fld.getName(), delim);
    }

    /** */
    public static String asOptional(String str, boolean optional) {
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

            Object res = Array.newInstance(compType, vals.length);

            for (int i = 0; i < vals.length; i++)
                Array.set(res, i, parseSingleVal(vals[i], compType));

            return (T)res;
        }

        return parseSingleVal(val, type);
    }

    /**
     * @param nodes Nodes.
     * @return Coordinator ID or null is {@code nodes} are empty.
     */
    public static @Nullable Collection<UUID> coordinatorOrNull(Map<UUID, T3<Boolean, Object, Long>> nodes) {
        return nodes.entrySet().stream()
            .filter(e -> !e.getValue().get1())
            .min(Comparator.comparingLong(e -> e.getValue().get3()))
            .map(e -> Collections.singleton(e.getKey()))
            .orElse(null);
    }

    /** */
    public static Collection<UUID> nodeOrNull(@Nullable UUID nodeId) {
        return nodeId == null ? null : Collections.singleton(nodeId);
    }

    /**
     * @param nodes Nodes.
     * @return Server nodes.
     */
    public static Collection<UUID> servers(Map<UUID, T3<Boolean, Object, Long>> nodes) {
        return nodes.entrySet().stream()
            .filter(e -> !e.getValue().get1())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    /**
     * Join input parameters with specified {@code delimeter} between them.
     *
     * @param delimeter Specified delimeter.
     * @param params Other input parameter.
     * @return Joined paramaters with specified {@code delimeter}.
     */
    public static String join(String delimeter, Object... params) {
        return join(new SB(), "", delimeter, params).toString();
    }

    /**
     * Join input parameters with specified {@code delimeter} between them and append to the end {@code delimeter}.
     *
     * @param sb Specified string builder.
     * @param sbDelimeter Delimeter between {@code sb} and appended {@code param}.
     * @param delimeter Specified delimeter.
     * @param params Other input parameter.
     * @return SB with appended to the end joined paramaters with specified {@code delimeter}.
     */
    public static SB join(SB sb, String sbDelimeter, String delimeter, Object... params) {
        if (!F.isEmpty(params)) {
            sb.a(sbDelimeter);

            for (Object par : params)
                sb.a(par).a(delimeter);

            sb.setLength(sb.length() - delimeter.length());
        }

        return sb;
    }

    /**
     * Prints exception messages to log
     *
     * @param exceptions map containing node ids and exceptions.
     * @param infoMsg single message to log.
     * @param printer Printer to use.
     * @return true if errors were printed.
     */
    public static boolean printErrors(Map<UUID, Exception> exceptions, String infoMsg, Consumer<String> printer) {
        if (!F.isEmpty(exceptions)) {
            printer.accept(infoMsg);

            for (Map.Entry<UUID, Exception> e : exceptions.entrySet()) {
                printer.accept(INDENT + "Node ID: " + e.getKey());

                printer.accept(INDENT + "Exception message:");
                printer.accept(DOUBLE_INDENT + e.getValue().getMessage());
                printer.accept("");
            }

            return true;
        }

        return false;
    }

    /**
     * Join input parameters with specified {@code delimeter} between them.
     *
     * @param delimeter Specified delimeter.
     * @param params Other input parameter.
     * @return Joined paramaters with specified {@code delimeter}.
     */
    public static String join(String delimeter, Object... params) {
        return join(new SB(), "", delimeter, params).toString();
    }

    /**
     * Join input parameters with specified {@code delimeter} between them and append to the end {@code delimeter}.
     *
     * @param sb Specified string builder.
     * @param sbDelimeter Delimeter between {@code sb} and appended {@code param}.
     * @param delimeter Specified delimeter.
     * @param params Other input parameter.
     * @return SB with appended to the end joined paramaters with specified {@code delimeter}.
     */
    public static SB join(SB sb, String sbDelimeter, String delimeter, Object... params) {
        if (!F.isEmpty(params)) {
            sb.a(sbDelimeter);

            for (Object par : params)
                sb.a(par).a(delimeter);

            sb.setLength(sb.length() - delimeter.length());
        }

        return sb;
    }

    /**
     * Prints exception messages to log
     *
     * @param exceptions map containing node ids and exceptions.
     * @param infoMsg single message to log.
     * @param printer Printer to use.
     * @return true if errors were printed.
     */
    public static boolean printErrors(Map<UUID, Exception> exceptions, String infoMsg, Consumer<String> printer) {
        if (F.isEmpty(exceptions))
            return false;

        printer.accept(infoMsg);

        for (Map.Entry<UUID, Exception> e : exceptions.entrySet()) {
            printer.accept(INDENT + "Node ID: " + e.getKey());

            printer.accept(INDENT + "Exception message:");
            printer.accept(DOUBLE_INDENT + e.getValue().getMessage());
            printer.accept("");
        }

        return true;
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
        else if (type == Integer.class || type == int.class) {
            int radix = radix(val);
            return (T)wrapNumberFormatException(
                () -> Integer.parseInt(radix == 10 ? val : val.substring(2), radix),
                val,
                Integer.class
            );
        }
        else if (type == Long.class || type == long.class) {
            int radix = radix(val);

            return (T)wrapNumberFormatException(
                () -> Long.parseLong(radix == 10 ? val : val.substring(2), radix),
                val,
                Long.class
            );
        }
        else if (type == Float.class || type == float.class)
            return (T)wrapNumberFormatException(() -> Float.parseFloat(val), val, Float.class);
        else if (type == Double.class || type == double.class)
            return (T)wrapNumberFormatException(() -> Double.parseDouble(val), val, Double.class);
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
        else if (type.isEnum()) {
            try {
                return (T)Enum.valueOf((Class<Enum>)type, val.toUpperCase());
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Can't parse value '" + val + "', expected type: " + type.getName());
            }
        }

        throw new IgniteException("Unsupported argument type: " + type.getName());
    }

    /** */
    private static int radix(String val) {
        return val.startsWith("0x") ? 16 : 10;
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
