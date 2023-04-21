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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.OneOf;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.lang.PeekableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import static java.util.Collections.singleton;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.formattedName;
import static org.apache.ignite.internal.management.api.CommandUtils.fromFormattedName;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterExample;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterName;

/**
 * Abstract class for management command invokers.
 */
public abstract class AbstractCommandInvoker {
    /**
     * Executes command with given arguments.
     *
     * @param cmd Command.
     * @param args String arguments.
     * @param printer Result printer.
     * @param <A> Argument type.
     * @param <R> Result type.
     * @param <T> Task type.
     */
    protected <A extends IgniteDataTransferObject, R, T extends ComputeTask<VisorTaskArgument<A>, R>> void execute(
        Command<A, R, T> cmd,
        Map<String, String> args,
        Consumer<String> printer
    ) {
        A arg;

        try {
            Function<Field, Object> paramProvider = fld -> Optional
                .ofNullable(args.get(fld.getName()))
                .map(v -> CommandUtils.parseVal(v, fld.getType()))
                .orElse(null);

            arg = argument(
                cmd.args(),
                (fld, idx) -> paramProvider.apply(fld),
                paramProvider
            );
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new IgniteException(e); //TODO: replace with print error to printer.
        }

        IgniteEx grid = grid();

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

        R res = compute.execute(cmd.task().getName(), new VisorTaskArgument<>(nodeIds, arg, false));

        cmd.printResult(arg, res, printer);
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
        A arg = argCls.newInstance();

        AtomicBoolean oneOfSet = new AtomicBoolean(false);

        Set<String> oneOfFields = arg.getClass().isAnnotationPresent(OneOf.class)
            ? new HashSet<>(Arrays.asList(arg.getClass().getAnnotation(OneOf.class).value()))
            : Collections.emptySet();

        BiConsumer<Field, Object> fldSetter = (fld, val) -> {
            if (val == null) {
                if (fld.getAnnotation(Argument.class).optional())
                    return;

                String name = fld.isAnnotationPresent(Positional.class)
                    ? parameterExample(fld, false)
                    : parameterName(fld);

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

        AtomicInteger idx = new AtomicInteger();

        visitCommandParams(
            arg.getClass(),
            fld -> fldSetter.accept(fld, positionalParamProvider.apply(fld, idx.getAndIncrement())),
            fld -> fldSetter.accept(fld, paramProvider.apply(fld)),
            (optionals, flds) -> flds.forEach(fld -> fldSetter.accept(fld, paramProvider.apply(fld)))
        );

        boolean oneOfRequired = arg.getClass().isAnnotationPresent(OneOf.class)
            && !arg.getClass().getAnnotation(OneOf.class).optional();

        if (oneOfRequired && !oneOfSet.get())
            throw new IllegalArgumentException("One of " + oneOfFields + " required");

        return arg;
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
    protected <A extends IgniteDataTransferObject> Command<A, ?, ?> command(
        CommandsRegistry root,
        PeekableIterator<String> iter,
        boolean isCli
    ) {
        if (!iter.hasNext())
            return (Command<A, ?, ?>)root;

        Command<A, ?, ?> cmd0 = (Command<A, ?, ?>)root;

        while (cmd0 instanceof CommandsRegistry && iter.hasNext()) {
            String name = iter.peek();

            if (!cmd0.getClass().isAnnotationPresent(CliPositionalSubcommands.class) && isCli) {
                if (!name.startsWith(PARAMETER_PREFIX))
                    break;

                name = name.substring(PARAMETER_PREFIX.length());
            }

            Command<A, ?, ?> cmd1 = ((CommandsRegistry)cmd0).command(fromFormattedName(name, CMD_WORDS_DELIM));

            if (cmd1 != null) {
                cmd0 = cmd1;

                iter.next();
            }
        }

        if (cmd0 instanceof CommandsRegistry && !((CommandsRegistry)cmd0).canBeExecuted()) {
            throw new IllegalArgumentException(
                "Command " + formattedName(cmd0.getClass().getSimpleName(), CMD_WORDS_DELIM) + " can't be executed"
            );
        }

        if (cmd0 == null)
            throw new IllegalArgumentException("Unknown command");

        return cmd0;
    }

    /**
     * Utility method. Scans argument class fields and visits each field representing command argument.
     *
     * @param argCls Argument class.
     * @param positionalParamVisitor Visitor of positional parameters.
     * @param namedParamVisitor Visitor of named parameters.
     * @param oneOfNamedParamVisitor Visitor of "one of" parameters.
     * @param <A> Argument type.
     */
    protected <A extends IgniteDataTransferObject> void visitCommandParams(
        Class<A> argCls,
        Consumer<Field> positionalParamVisitor,
        Consumer<Field> namedParamVisitor,
        BiConsumer<Boolean, List<Field>> oneOfNamedParamVisitor
    ) {
        List<Field> positionalParams = new ArrayList<>();
        List<Field> namedParams = new ArrayList<>();

        OneOf oneOf = argCls.getAnnotation(OneOf.class);

        Set<String> oneOfNames = oneOf != null
            ? new HashSet<>(Arrays.asList(oneOf.value()))
            : Collections.emptySet();

        List<Field> oneOfFlds = new ArrayList<>();

        Class<? extends IgniteDataTransferObject> clazz0 = argCls;

        while (clazz0 != IgniteDataTransferObject.class) {
            Field[] flds = clazz0.getDeclaredFields();

            for (Field fld : flds) {
                if (oneOfNames.contains(fld.getName()))
                    oneOfFlds.add(fld);
                else if (fld.isAnnotationPresent(Positional.class))
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

    /** @return Local node. */
    public abstract IgniteEx grid();
}
