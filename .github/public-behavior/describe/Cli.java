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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.commandline.ArgumentParser;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.lang.IgniteExperimental;

/** Describes {@code control.sh} commands, their parameters and the common options. */
class Cli implements Collector {
    /** */
    private static final String AREA = "CLI";

    /** Field of {@link ArgumentParser} with the common options. */
    private static final String COMMON_OPTIONS = "common";

    /** Field of {@link CommandHandler.CommandName} with the name that the help prints. */
    private static final String PRINTED_NAME = "name";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        IgniteCommandRegistry registry = new IgniteCommandRegistry();

        registry.commands().forEachRemaining(e -> command(tree, out, e.getValue(), new ArrayList<>()));

        Field common = ArgumentParser.class.getDeclaredField(COMMON_OPTIONS);

        common.setAccessible(true);

        for (CLIArgument<?> arg : (List<CLIArgument<?>>)common.get(new ArgumentParser(null, registry, null))) {
            out.add(AREA, "control.sh " + arg.name(), "common option, " + (arg.optional() ? "optional" : "required") + ", "
                + arg.type().getTypeName() + (arg.isSensitive() ? ", sensitive" : ""), "", tree.where(ArgumentParser.class, null, -1, false));
        }
    }

    /**
     * Describes a command and its subcommands. {@code control.sh} refuses a command marked experimental unless
     * {@code --enable-experimental} is given, and it checks the mark of the command itself, not of its parents. So that mark is
     * part of the command. A command is deprecated if it or a parent is: removing the parent removes the command. The
     * deprecation hint of the command and its parameters points at that annotation.
     */
    private static void command(BuiltTree tree, Declarations out, Command<?, ?> cmd, List<Command<?, ?>> parents) {
        CommandHandler.CommandName name = new CommandHandler.CommandName();

        parents.forEach(name);
        name.accept(cmd);

        String path = "control.sh" + printedName(name);
        boolean experimental = cmd.getClass().isAnnotationPresent(IgniteExperimental.class);
        List<Command<?, ?>> chain = new ArrayList<>(parents);

        chain.add(cmd);
        Collections.reverse(chain);

        Class<?> deprecatedBy = chain.stream().map(Object::getClass).filter(c -> c.isAnnotationPresent(Deprecated.class))
            .findFirst().orElse(null);
        String flags = Declarations.flags(deprecatedBy != null, experimental);
        String deprecation = deprecatedBy == null ? null : tree.where(deprecatedBy, null, -1, true);

        if (CommandUtils.executable(cmd)) {
            out.add(AREA, path, "command" + (experimental ? ", experimental" : "") + (deprecatedBy != null ? ", deprecated" : ""),
                flags, tree.where(cmd.getClass(), null, -1, false), deprecation, "");

            int[] pos = {0};

            CommandUtils.visitCommandParams(cmd.argClass(),
                fld -> parameter(tree, out, path, fld, ++pos[0], false, flags, deprecation),
                fld -> parameter(tree, out, path, fld, 0, false, flags, deprecation),
                (grp, flds) -> {
                    List<String> positional = new ArrayList<>();
                    Set<String> named = new TreeSet<>();

                    for (Field fld : flds) {
                        if (fld.isAnnotationPresent(Positional.class)) {
                            positional.add("<arg " + (pos[0] + 1) + '>');
                            parameter(tree, out, path, fld, ++pos[0], true, flags, deprecation);
                        }
                        else {
                            named.add(CommandUtils.toFormattedFieldName(fld));
                            parameter(tree, out, path, fld, 0, true, flags, deprecation);
                        }
                    }

                    positional.addAll(named);

                    out.add(AREA, path + ' ' + String.join("|", positional), "group, " + (grp.optional() ? "optional" : "required")
                        + (grp.onlyOneOf() ? ", only one of" : ""), flags, tree.where(cmd.argClass(), null, -1, false), deprecation, "");
                });
        }

        if (cmd instanceof CommandsRegistry) {
            List<Command<?, ?>> parents0 = new ArrayList<>(parents);

            parents0.add(cmd);

            ((CommandsRegistry<?, ?>)cmd).commands().forEachRemaining(e -> command(tree, out, e.getValue(), parents0));
        }
    }

    /** @return Command name as {@code control.sh} prints it in the help. */
    private static String printedName(CommandHandler.CommandName name) {
        try {
            Field fld = CommandHandler.CommandName.class.getDeclaredField(PRINTED_NAME);

            fld.setAccessible(true);

            return fld.get(name).toString();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Describes a command parameter. Positional parameters are identified by position, named ones by name. The parser treats
     * a named parameter of a group as optional and checks the group instead, so such a parameter is described as part of
     * its group. The group lists its named parameters in alphabetical order, because their order does not matter to the parser.
     * An enum parameter is described by the names it accepts: the enum class and the order of its constants are internal.
     *
     * @param deprecation Location of the deprecation of the command, or {@code null}.
     */
    private static void parameter(BuiltTree tree, Declarations out, String path, Field fld, int pos, boolean grouped, String flags,
        String deprecation) {
        Argument arg = fld.getAnnotation(Argument.class);
        Class<?> type = fld.getType().isArray() ? fld.getType().getComponentType() : fld.getType();

        String values = type.isEnum()
            ? Arrays.stream(type.getEnumConstants()).map(c -> ((Enum<?>)c).name()).sorted()
                .collect(Collectors.joining("|", ", values ", ""))
            : "";
        String typeName = type.isEnum() ? "enum" + (fld.getType().isArray() ? "[]" : "") : fld.getType().getTypeName();
        String required = pos == 0 && grouped ? "in a group, " : arg != null && arg.optional() ? "optional, " : "required, ";

        out.add(AREA, path + ' ' + (pos > 0 ? "<arg " + pos + '>' : CommandUtils.toFormattedFieldName(fld)),
            required + typeName + values + (arg != null && arg.sensitive() ? ", sensitive" : ""), flags,
            tree.where(fld.getDeclaringClass(), fld.getName(), -1, false), deprecation, "");
    }
}
