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

package org.apache.ignite.internal.management;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.lang.PeekableIterator;
import org.apache.ignite.internal.util.typedef.T3;
import static java.util.Collections.singleton;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.fromFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterExample;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedFieldName;

/**
 * Abstract class for management command invokers.
 */
public abstract class AbstractCommandInvoker {
    /**
     * @param cmd Command.
     * @param arg Command argument.
     * @param nodes Cluster nodes.
     * @param dflt Default node.
     * @param <A> Argument type.
     * @return Nodes to execute command on.
     */
    protected <A extends IgniteDataTransferObject, R> Collection<UUID> commandNodes(
        ComputeCommand<A, ?> cmd,
        A arg,
        Map<UUID, T3<Boolean, Object, Long>> nodes,
        UUID dflt
    ) {
        Collection<UUID> nodeIds = cmd.nodes(nodes, arg);

        if (nodeIds == null)
            return singleton(dflt);

        for (UUID id : nodeIds) {
            if (!nodes.containsKey(id))
                throw new IllegalArgumentException("Node with id=" + id + " not found.");
        }

        return nodeIds;
    }

    /**
     * Fill and vaildate command argument.
     *
     * @param argCls Argument class.
     * @param positionalParamProvider Provider of positional parameters.
     * @param paramProvider Provider of named parameters.
     * @return Argument filled with parameters.
     * @param <A> Argument type.
     * @throws InstantiationException If failed.
     * @throws IllegalAccessException If failed.
     */
    protected <A extends IgniteDataTransferObject> A argument(
        Class<A> argCls,
        BiFunction<Field, Integer, Object> positionalParamProvider,
        Function<Field, Object> paramProvider
    ) throws InstantiationException, IllegalAccessException {
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

        if (!arg.grpOptional() && !arg.grpFldExists)
            throw new IllegalArgumentException("One of " + arg.oneOfFlds + " required");

        return arg.res;
    }

    /** */
    private static class ArgumentState<A extends IgniteDataTransferObject> implements BiConsumer<Field, Object> {
        /** */
        final A res;

        /** */
        final ArgumentGroup argGrp;

        /** */
        boolean grpFldExists;

        /** */
        int idx;

        /** */
        final Set<String> oneOfFlds;

        /** */
        public ArgumentState(Class<A> argCls) throws InstantiationException, IllegalAccessException {
            res = argCls.newInstance();
            argGrp = argCls.getAnnotation(ArgumentGroup.class);
            oneOfFlds = argGrp == null
                ? Collections.emptySet()
                : new HashSet<>(Arrays.asList(argGrp.value()));
        }

        /** */
        public boolean grpOptional() {
            return argGrp != null && argGrp.optional();
        }

        /** */
        private int nextIdx() {
            int idx0 = idx;

            idx++;

            return idx0;
        }

        /** {@inheritDoc} */
        @Override public void accept(Field fld, Object val) {
            if (val == null) {
                if (fld.getAnnotation(Argument.class).optional())
                    return;

                String name = fld.isAnnotationPresent(Positional.class)
                    ? parameterExample(fld, false)
                    : toFormattedFieldName(fld);

                throw new IllegalArgumentException("Argument " + name + " required.");
            }

            if (oneOfFlds.contains(fld.getName())) {
                if (grpFldExists && (argGrp != null && argGrp.onlyOneOf()))
                    throw new IllegalArgumentException("Only one of " + oneOfFlds + " allowed");

                grpFldExists = true;
            }

            try {
                res.getClass().getMethod(fld.getName(), fld.getType()).invoke(res, val);
            }
            catch (NoSuchMethodException | IllegalAccessException e) {
                throw new IgniteException(e);
            }
            catch (InvocationTargetException e) {
                if (e.getTargetException() != null && e.getTargetException() instanceof RuntimeException)
                    throw (RuntimeException)e.getTargetException();
            }
        }
    }

    /**
     * Get command from hierarchical root.
     *
     * @param root Root command.
     * @param iter Iterator of commands names.
     * @param isCli {@code True} if command parsed in cli utility.
     * @return Command to execute.
     * @param <A> Argument type.
     */
    protected <A extends IgniteDataTransferObject> Command<A, ?> command(
        CommandsRegistry<?, ?> root,
        PeekableIterator<String> iter,
        boolean isCli
    ) {
        if (!iter.hasNext())
            return (Command<A, ?>)root;

        Command<A, ?> cmd0 = (Command<A, ?>)root;

        while (cmd0 instanceof CommandsRegistry && iter.hasNext()) {
            String name = iter.peek();

            if (!cmd0.getClass().isAnnotationPresent(CliPositionalSubcommands.class) && isCli) {
                if (!name.startsWith(PARAMETER_PREFIX))
                    break;

                name = name.substring(PARAMETER_PREFIX.length());
            }

            Command<A, ?> cmd1 =
                (Command<A, ?>)((CommandsRegistry<?, ?>)cmd0).command(fromFormattedCommandName(name, PARAM_WORDS_DELIM));

            if (cmd1 == null)
                break;

            cmd0 = cmd1;

            iter.next();
        }

        return cmd0;
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
    protected <A extends IgniteDataTransferObject> void visitCommandParams(
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

        ArgumentGroup argGrp = argCls.getAnnotation(ArgumentGroup.class);

        Set<String> grpNames = argGrp != null
            ? new HashSet<>(Arrays.asList(argGrp.value()))
            : Collections.emptySet();

        List<Field> grpFlds = new ArrayList<>();

        // Iterates classes from the roots.
        for (int i = classes.size() - 1; i >= 0; i--) {
            Field[] flds = classes.get(i).getDeclaredFields();

            for (Field fld : flds) {
                if (grpNames.contains(fld.getName()))
                    grpFlds.add(fld);
                else if (fld.isAnnotationPresent(Positional.class))
                    positionalParams.add(fld);
                else if (fld.isAnnotationPresent(Argument.class))
                    namedParams.add(fld);
            }
        }

        positionalParams.forEach(positionalParamVisitor);

        namedParams.forEach(namedParamVisitor);

        if (argGrp != null)
            argumentGroupVisitor.accept(argGrp, grpFlds);
    }

    /** @return Local node. */
    public abstract IgniteEx grid();
}
