/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.argument.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.GridStringBuilder;

import static java.util.stream.Collectors.toSet;

/**
 * Parser for command line arguments.
 */
public class CLIArgumentParser {
    /** */
    private final List<CLIArgument<?>> positionalArgCfg;

    /** */
    private final Map<String, CLIArgument<?>> namedArgCfg = new LinkedHashMap<>();

    /** */
    private final List<Object> parsedPositionalArgs = new ArrayList<>();

    /** */
    private final Map<String, Object> parsedNamedArgs = new HashMap<>();

    /** */
    private final boolean failOnUnknown;

    /** */
    public CLIArgumentParser(List<CLIArgument<?>> namedArgCfg) {
        this(Collections.emptyList(), namedArgCfg, true);
    }

    /** */
    public CLIArgumentParser(
        List<CLIArgument<?>> positionalArgConfig,
        List<CLIArgument<?>> namedArgCfg,
        boolean failOnUnknown
    ) {
        this.positionalArgCfg = positionalArgConfig;

        for (CLIArgument<?> cliArgument : namedArgCfg)
            this.namedArgCfg.put(cliArgument.name(), cliArgument);

        this.failOnUnknown = failOnUnknown;
    }

    /**
     * Parses arguments using iterator. Parsed argument value are available through {@link #get(CLIArgument)}
     * and {@link #get(String)}.
     *
     * @param argsIter Iterator.
     */
    public void parse(Iterator<String> argsIter) {
        Set<String> obligatoryArgs =
            namedArgCfg.values().stream().filter(a -> !a.optional()).map(CLIArgument::name).collect(toSet());

        int positionalIdx = 0;

        while (argsIter.hasNext()) {
            String arg = argsIter.next();

            CLIArgument<?> cliArg = namedArgCfg.get(arg);

            if (cliArg == null) {
                if (positionalIdx < positionalArgCfg.size()) {
                    cliArg = positionalArgCfg.get(positionalIdx);

                    parsedPositionalArgs.add(parseVal(arg, cliArg.type()));
                }
                else if (failOnUnknown)
                    throw new IgniteException("Unexpected argument: " + arg);

                continue;
            }

            if (cliArg.type().equals(Boolean.class))
                parsedNamedArgs.put(cliArg.name(), true);
            else {
                if (!argsIter.hasNext())
                    throw new IgniteException("Please specify a value for argument: " + arg);

                String strVal = argsIter.next();

                parsedNamedArgs.put(cliArg.name(), parseVal(strVal, cliArg.type()));
            }

            obligatoryArgs.remove(cliArg.name());
        }

        if (!obligatoryArgs.isEmpty())
            throw new IgniteException("Mandatory argument(s) missing: " + obligatoryArgs);
    }

    /** */
    private <T> T parseVal(String val, Class<T> type) {
        if (type == String.class)
            return (T)val;
        else if (type == String[].class)
            return (T)val.split(",");
        else if (type == Integer.class)
            return (T)wrapNumberFormatException(() -> Integer.parseInt(val), val, Integer.class);
        else if (type == Long.class)
            return (T)wrapNumberFormatException(() -> Long.parseLong(val), val, Long.class);
        else if (type == UUID.class)
            return (T)UUID.fromString(val);

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
    private Object wrapNumberFormatException(Supplier<Object> closure, String val, Class<? extends Number> expectedType) {
        try {
            return closure.get();
        }
        catch (NumberFormatException e) {
            throw new NumberFormatException("Can't parse number '" + val + "', expected type: " + expectedType.getName());
        }
    }

    /**
     * Get parsed argument value.
     *
     * @param arg Argument configuration.
     * @param <T> Value type.
     * @return Value.
     */
    public <T> T get(CLIArgument<?> arg) {
        Object val = parsedNamedArgs.get(arg.name());

        if (val == null)
            return (T)arg.defaultValueSupplier().apply(this);
        else
            return (T)val;
    }

    /**
     * Get parsed argument value.
     *
     * @param name Argument name.
     * @param <T> Value type.
     * @return Value.
     */
    public <T> T get(String name) {
        CLIArgument<?> arg = namedArgCfg.get(name);

        if (arg == null)
            throw new IgniteException("No such argument: " + name);

        return get(arg);
    }

    /**
     * Get parsed positional argument value.
     *
     * @param position Argument position.
     * @param <T> Value type.
     * @return Value.
     */
    public <T> T get(int position) {
        if (parsedPositionalArgs.size() < position)
            return null;

        return (T)parsedPositionalArgs.get(position);
    }

    /**
     * Returns usage description.
     *
     * @return Usage.
     */
    public String usage() {
        return usage(true, "Usage:", "");
    }

    /**
     * Returns usage description.
     *
     * @return Usage.
     */
    public String usage(boolean includeDetailedDesc, String utilName, String additional) {
        GridStringBuilder sb = new GridStringBuilder(utilName + " ");

        for (CLIArgument<?> arg : namedArgCfg.values())
            sb.a(argNameForUsage(arg, includeDetailedDesc)).a(" ");

        if (includeDetailedDesc) {
            for (CLIArgument<?> arg : namedArgCfg.values()) {
                Object dfltVal = null;

                try {
                    dfltVal = arg.defaultValueSupplier().apply(this);
                }
                catch (Exception ignored) {
                    /* No op. */
                }

                sb.a("\n\n").a(arg.name()).a(": ").a(arg.usage());

                if (arg.optional())
                    sb.a(" Default value: ").a(dfltVal instanceof String[] ? Arrays.toString((Object[])dfltVal) : dfltVal);
            }
        }

        sb.a(additional);

        return sb.toString();
    }

    /** */
    private String argNameForUsage(CLIArgument<?> arg, boolean inclueDetailedDesc) {
        String name = arg.name();

        if (!inclueDetailedDesc && arg.type() != Boolean.class && arg.type() != boolean.class)
            name += " " + arg.usage();

        if (arg.optional())
            return "[" + name + "]";
        else
            return name;
    }
}
