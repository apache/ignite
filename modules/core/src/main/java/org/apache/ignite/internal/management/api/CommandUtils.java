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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.VisorTaskResult;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.api.Command.CMD_NAME_POSTFIX;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.nodeIds;

/**
 * Utility class for management commands.
 */
public class CommandUtils {
    /** CLI named parameter prefix. */
    public static final String NAME_PREFIX = "--";

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
     * @param cmd Command class.
     * @return Formatted command name.
     */
    public static String cmdText(Command<?, ?> cmd) {
        return NAME_PREFIX + toFormattedCommandName(cmd.getClass(), CMD_WORDS_DELIM);
    }

    /**
     * @param cmdCls Command class.
     * @param parent Parent registry.
     * @return Command key without parent prefix.
     */
    public static String cmdKey(Class<?> cmdCls, Class<? extends CommandsRegistry<?, ?>> parent) {
        String name = cmdCls.getSimpleName();

        if (parent != null) {
            String parentName = parent.getSimpleName();

            parentName = parentName.substring(0, parentName.length() - CMD_NAME_POSTFIX.length());

            if (!name.startsWith(parentName)) {
                throw new IllegalArgumentException(
                    "Command class name must starts with parent name [parent=" + parentName + ']');
            }

            name = name.substring(parentName.length());
        }

        if (!name.endsWith(CMD_NAME_POSTFIX))
            throw new IllegalArgumentException("Command class name must ends with 'Command'");

        return name.substring(0, name.length() - CMD_NAME_POSTFIX.length());
    }

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
        return (fld.getAnnotation(Argument.class).withoutPrefix() ? "" : NAME_PREFIX)
            + toFormattedFieldName(fld, CMD_WORDS_DELIM);
    }

    /**
     * @param argCls Argument class.
     * @param flds Fields to format.
     * @return Formatted names.
     */
    public static Set<String> toFormattedNames(Class<?> argCls, Set<String> flds) {
        return flds.stream()
            .map(name -> U.findField(argCls, name))
            .map(CommandUtils::toFormattedFieldName)
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
        if (isBoolean(fld.getType()))
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

    /** */
    public static boolean isBoolean(Class<?> cls) {
        return cls == Boolean.class || cls == boolean.class;
    }

    /**
     * @param fld Field.
     * @param delim Words delimeter.
     * @return Name of the field.
     */
    private static String toFormattedFieldName(Field fld, char delim) {
        if (fld.isAnnotationPresent(Positional.class))
            return toFormattedName(fld.getName(), delim);

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
        if (type.isArray() && type != char[].class) {
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
     * Utility method. Scans argument class fields and visits each field representing command argument.
     *
     * @param argCls Argument class.
     * @param positionalParamVisitor Visitor of positional parameters.
     * @param namedParamVisitor Visitor of named parameters.
     * @param argumentGroupVisitor Visitor of "one of" parameters.
     * @param <A> Argument type.
     */
    public static <A extends IgniteDataTransferObject> void visitCommandParams(
        Class<A> argCls,
        Consumer<Field> positionalParamVisitor,
        Consumer<Field> namedParamVisitor,
        BiConsumer<ArgumentGroup, List<Field>> argumentGroupVisitor
    ) {
        Class<? extends IgniteDataTransferObject> clazz0 = argCls;

        List<Class<? extends IgniteDataTransferObject>> classes = new ArrayList<>();

        while (clazz0 != IgniteDataTransferObject.class) {
            classes.add(clazz0);

            clazz0 = (Class<? extends IgniteDataTransferObject>)clazz0.getSuperclass();
        }

        List<Field> positionalParams = new ArrayList<>();
        List<Field> namedParams = new ArrayList<>();

        List<ArgumentGroup> argGprs = argumentGroups(argCls);

        List<Set<String>> grpNames = argumentGroupsValues(argGprs);

        List<List<Field>> grpFlds = grpNames.isEmpty() ? Collections.emptyList() : new ArrayList<>(grpNames.size());

        grpNames.forEach(gf -> grpFlds.add(grpFlds.size(), null));

        // Iterates classes from the roots.
        for (int i = classes.size() - 1; i >= 0; i--) {
            Field[] flds = classes.get(i).getDeclaredFields();

            for (Field fld : flds) {
                int argGrpIdx = argumentGroupIdx(grpNames, fld.getName());

                if (argGrpIdx >= 0) {
                    if (grpFlds.get(argGrpIdx) == null)
                        grpFlds.set(argGrpIdx, new ArrayList<>());

                    grpFlds.get(argGrpIdx).add(fld);
                }
                else if (fld.isAnnotationPresent(Positional.class))
                    positionalParams.add(fld);
                else if (fld.isAnnotationPresent(Argument.class))
                    namedParams.add(fld);
            }
        }

        positionalParams.forEach(positionalParamVisitor);

        namedParams.forEach(namedParamVisitor);

        for (int i = 0; i < grpFlds.size(); ++i)
            argumentGroupVisitor.accept(argGprs.get(i), grpFlds.get(i));
    }

    /**
     * @return List of declared {@link ArgumentGroup} at {@code cls}. Singleton list if only one argument group is
     * declared. Empty list if no argument group is declared.
     */
    private static List<ArgumentGroup> argumentGroups(Class<?> cls) {
        ArgumentGroup singleGrp = cls.getAnnotation(ArgumentGroup.class);

        if (singleGrp != null) {
            assert cls.getAnnotation(ArgumentGroupsHolder.class) == null;

            return Collections.singletonList(singleGrp);
        }

        ArgumentGroupsHolder grps = cls.getAnnotation(ArgumentGroupsHolder.class);

        return grps == null ? Collections.emptyList() : Arrays.asList(grps.value());
    }

    /**
     * @return Sets list of {@link ArgumentGroup#value()} declared at {@code cls}.
     */
    public static List<Set<String>> argumentGroupsValues(Class<?> cls) {
        return argumentGroupsValues(argumentGroups(cls));
    }

    /**
     * @return Sets list of {@link ArgumentGroup#value()} holding in {@code argGrps}.
     * @see #argumentGroupsValues(Class)
     */
    public static List<Set<String>> argumentGroupsValues(List<ArgumentGroup> argGrps) {
        List<Set<String>> res = argGrps.stream().map(grp -> new HashSet<>(Arrays.asList(grp.value())))
            .collect(Collectors.toList());

        // Checks that argument groups only unique values.
        assert F.flatCollections(res).stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
            .entrySet().stream().noneMatch(e -> e.getValue() > 1) : "Argument groups " + argGrps + " have not unique arguments";

        return res;
    }

    /**
     * @return Index of first value set in {@code argGrpValues} containing {@code name}. -1 if not found.
     * @see #argumentGroupsValues(Class)
     */
    public static int argumentGroupIdx(List<Set<String>> argGrpValues, String name) {
        for (int i = 0; i < argGrpValues.size(); ++i) {
            if (argGrpValues.get(i).contains(name))
                return i;
        }

        return -1;
    }

    /**
     * @param cmd Command.
     * @return {@code True} if command has described parameters.
     */
    public static boolean hasDescribedParameters(Command<?, ?> cmd) {
        AtomicBoolean res = new AtomicBoolean();

        visitCommandParams(
            cmd.argClass(),
            fld -> res.compareAndSet(false, hasDescription(fld)),
            fld -> res.compareAndSet(false, hasDescription(fld)),
            (argGrp, flds) -> flds.forEach(fld -> res.compareAndSet(false, hasDescription(fld)))
        );

        return res.get();
    }

    /** @return {@code True} if argument has description. */
    public static boolean hasDescription(Field fld) {
        return !fld.getAnnotation(Argument.class).description().isEmpty() ||
            fld.isAnnotationPresent(EnumDescription.class);
    }

    /**
     * @param nodes Nodes.
     * @return Coordinator ID or null is {@code nodes} are empty.
     */
    public static @Nullable Collection<ClusterNode> coordinatorOrNull(Collection<ClusterNode> nodes) {
        return nodes.stream()
            .filter(n -> !n.isClient())
            .min(Comparator.comparingLong(ClusterNode::order))
            .map(Collections::singleton)
            .orElse(null);
    }

    /** */
    public static @Nullable Collection<ClusterNode> nodeOrNull(@Nullable UUID nodeId, Collection<ClusterNode> nodes) {
        return nodeId == null ? null : node(nodeId, nodes);
    }

    /** */
    public static Collection<ClusterNode> nodeOrAll(@Nullable UUID nodeId, Collection<ClusterNode> nodes) {
        return nodeId == null ? nodes : node(nodeId, nodes);
    }

    /** */
    public static Collection<ClusterNode> nodes(UUID[] nodeIds, Collection<ClusterNode> nodes) {
        List<ClusterNode> res = new ArrayList<>();

        for (UUID nodeId : nodeIds)
            res.addAll(node(nodeId, nodes));

        return res;
    }

    /** */
    public static List<ClusterNode> node(UUID nodeId, Collection<ClusterNode> nodes) {
        for (ClusterNode node : nodes) {
            if (node.id().equals(nodeId))
                return Collections.singletonList(node);
        }

        throw new IllegalArgumentException("Node with id=" + nodeId + " not found.");
    }

    /**
     * @param nodes Nodes.
     * @return Server nodes.
     */
    public static Collection<ClusterNode> servers(Collection<ClusterNode> nodes) {
        return nodes.stream()
            .filter(e -> !e.isClient())
            .collect(Collectors.toList());
    }

    /**
     * @param cmd Command.
     * @return {@code True} if command can be executed, {@code false} otherwise.
     */
    public static boolean executable(Command<?, ?> cmd) {
        return cmd instanceof NativeCommand
            || cmd instanceof ComputeCommand
            || cmd instanceof HelpCommand
            || cmd instanceof BeforeNodeStartCommand
            || cmd instanceof OfflineCommand;
    }

    /**
     * @param ignite Ignite node.
     * @return Collection of cluster nodes.
     */
    public static Collection<ClusterNode> nodes(
        @Nullable IgniteClient client,
        @Nullable Ignite ignite
    ) {
        if (client != null)
            return client.cluster().nodes();

        return ignite.cluster().nodes();
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

        exceptions.forEach((nodeId, err) -> printNodeError(printer, nodeId, null, err));

        return true;
    }

    /**
     * Prints single node exception message to the log.
     *
     * @param printer Printer to use.
     * @param nodeId Node id.
     * @param consistentId Node consistent id.
     * @param err Exception.
     */
    public static void printNodeError(
        Consumer<String> printer,
        UUID nodeId,
        @Nullable Object consistentId,
        Exception err
    ) {
        printer.accept(INDENT + "Node ID: " + nodeId + (consistentId == null ? "" : " [consistentId='" + consistentId + "']"));
        printer.accept(INDENT + "Exception message:");
        printer.accept(DOUBLE_INDENT + err.getMessage());
        printer.accept("");
    }

    /** */
    public static boolean experimental(Command<?, ?> cmd) {
        return cmd.getClass().isAnnotationPresent(IgniteExperimental.class);
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
        if (isBoolean(type))
            return (T)(Boolean)Boolean.parseBoolean(val);
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
        else if (type == char[].class)
            return (T)val.toCharArray();

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

    /**
     * Fill and vaildate command argument.
     *
     * @param argCls Argument class.
     * @param positionalParamProvider Provider of positional parameters.
     * @param paramProvider Provider of named parameters.
     * @return Argument filled with parameters.
     * @param <A> Argument type.
     */
    public static <A extends IgniteDataTransferObject> A argument(
        Class<A> argCls,
        BiFunction<Field, Integer, Object> positionalParamProvider,
        Function<Field, Object> paramProvider
    ) {
        try {
            ArgumentState<A> arg = new ArgumentState<>(argCls);

            visitCommandParams(
                argCls,
                fld -> arg.accept(fld, positionalParamProvider.apply(fld, arg.nextIdx())),
                fld -> arg.accept(fld, paramProvider.apply(fld)),
                (argGrp, flds) -> flds.forEach(fld -> {
                    if (fld.isAnnotationPresent(Positional.class))
                        arg.accept(fld, positionalParamProvider.apply(fld, arg.nextIdx()));
                    else
                        arg.accept(fld, paramProvider.apply(fld));
                })
            );

            for (int grpIdx = 0; grpIdx < arg.argGrps.size(); ++grpIdx) {
                if (!arg.argGrps.get(grpIdx).optional() && !arg.grpFldExists[grpIdx]) {
                    throw new IllegalArgumentException("One of " + toFormattedNames(argCls, arg.grpdFlds.get(grpIdx))
                        + " required");
                }
            }

            return arg.res;
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public static <A, R> R execute(
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        Class<? extends VisorMultiNodeTask<A, R, ?>> taskCls,
        A arg,
        Collection<ClusterNode> nodes
    ) throws Exception {
        Collection<UUID> nodesIds = nodeIds(nodes);

        if (client != null) {
            try {
                return client.compute(client.cluster().forNodes(client.cluster().nodes()))
                    .<VisorTaskArgument<A>, VisorTaskResult<R>>execute(
                        taskCls.getName(),
                        new VisorTaskArgument<>(nodesIds, arg, false)
                    ).result();
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }
        }

        return ignite
            .compute(ignite.cluster())
            .execute(taskCls, new VisorTaskArgument<>(nodesIds, arg, false))
            .result();
    }

    /** */
    private static class ArgumentState<A extends IgniteDataTransferObject> implements BiConsumer<Field, Object> {
        /** */
        private final A res;

        /** */
        private final List<ArgumentGroup> argGrps;

        /** */
        private final @Nullable boolean[] grpFldExists;

        /** */
        private int idx;

        /** */
        private final List<Set<String>> grpdFlds;

        /** */
        public ArgumentState(Class<A> argCls) throws InstantiationException, IllegalAccessException {
            res = argCls.newInstance();

            argGrps = argumentGroups(argCls);

            grpdFlds = argumentGroupsValues(argGrps);

            grpFldExists = argGrps.isEmpty() ? null : new boolean[argGrps.size()];
        }

        /** */
        private int nextIdx() {
            int idx0 = idx;

            idx++;

            return idx0;
        }

        /** {@inheritDoc} */
        @Override public void accept(Field fld, Object val) {
            int argGrpIdx = argumentGroupIdx(grpdFlds, fld.getName());

            assert argGrpIdx < argGrps.size();

            ArgumentGroup argGrp = argGrpIdx < 0 ? null : argGrps.get(argGrpIdx);

            if (val == null) {
                if (argGrp != null || fld.getAnnotation(Argument.class).optional())
                    return;

                String name = fld.isAnnotationPresent(Positional.class)
                    ? parameterExample(fld, false)
                    : toFormattedFieldName(fld);

                throw new IllegalArgumentException("Argument " + name + " required.");
            }

            if (Objects.equals(val, get(fld)))
                return;

            if (argGrp != null) {
                assert grpFldExists != null;

                if (grpFldExists[argGrpIdx] && argGrp.onlyOneOf()) {
                    throw new IllegalArgumentException(
                        "Only one of " + toFormattedNames(res.getClass(), grpdFlds.get(argGrpIdx)) + " allowed"
                    );
                }

                grpFldExists[argGrpIdx] = true;
            }

            set(fld, val);
        }

        /** */
        private Object get(Field fld) {
            try {
                return res.getClass().getMethod(fld.getName()).invoke(res);
            }
            catch (NoSuchMethodException | IllegalAccessException e) {
                throw new IgniteException(e);
            }
            catch (InvocationTargetException e) {
                if (e.getTargetException() != null && e.getTargetException() instanceof RuntimeException)
                    throw (RuntimeException)e.getTargetException();

                throw new IgniteException(e);
            }
        }

        /** */
        private void set(Field fld, Object val) {
            try {
                res.getClass().getMethod(fld.getName(), fld.getType()).invoke(res, val);
            }
            catch (NoSuchMethodException | IllegalAccessException e) {
                throw new IgniteException(e);
            }
            catch (InvocationTargetException e) {
                if (e.getTargetException() != null && e.getTargetException() instanceof RuntimeException)
                    throw (RuntimeException)e.getTargetException();

                throw new IgniteException(e);
            }
        }
    }
}
