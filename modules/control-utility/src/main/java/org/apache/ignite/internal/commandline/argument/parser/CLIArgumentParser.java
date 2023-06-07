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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.GridStringBuilder;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.management.api.CommandUtils.NAME_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.parseVal;

/**
 * Parser for command line arguments.
 */
public class CLIArgumentParser {
    /** */
    private final List<CLIArgument<?>> positionalArgCfg;

    /** */
    private final Map<String, CLIArgument<?>> argConfiguration = new LinkedHashMap<>();

    /** */
    private final List<Object> parsedPositionalArgs = new ArrayList<>();

    /** */
    private final Map<String, Object> parsedArgs = new HashMap<>();

    /** */
    public CLIArgumentParser(List<CLIArgument<?>> argConfiguration) {
        this(Collections.emptyList(), argConfiguration);
    }

    /** */
    public CLIArgumentParser(
        List<CLIArgument<?>> positionalArgConfig,
        List<CLIArgument<?>> argConfiguration
    ) {
        this.positionalArgCfg = positionalArgConfig;

        for (CLIArgument<?> cliArgument : argConfiguration)
            this.argConfiguration.put(cliArgument.name(), cliArgument);
    }

    /**
     * Parses arguments using iterator. Parsed argument value are available through {@link #get(CLIArgument)}
     * and {@link #get(String)}.
     *
     * @param argsIter Iterator.
     */
    public void parse(Iterator<String> argsIter) {
        Set<String> obligatoryArgs =
            argConfiguration.values().stream().filter(a -> !a.optional()).map(CLIArgument::name).collect(toSet());

        int positionalIdx = 0;

        while (argsIter.hasNext()) {
            String arg = argsIter.next();

            CLIArgument<?> cliArg = argConfiguration.get(arg.toLowerCase());

            if (cliArg == null) {
                if (positionalIdx < positionalArgCfg.size()) {
                    cliArg = positionalArgCfg.get(positionalIdx);

                    Object value = parseVal(arg, cliArg.type());

                    ((CLIArgument<Object>)cliArg).validator().accept(cliArg.name(), value);

                    parsedPositionalArgs.add(value);

                    positionalIdx++;
                }
                else
                    throw new IllegalArgumentException("Unexpected argument: " + arg);

                continue;
            }
            else if (parsedArgs.get(cliArg.name()) != null)
                throw new IllegalArgumentException(cliArg.name() + " argument specified twice");

            boolean bool = cliArg.type().equals(Boolean.class) || cliArg.type().equals(boolean.class);

            if (!bool && !argsIter.hasNext())
                throw new IllegalArgumentException("Please specify a value for argument: " + arg);

            String strVal = bool ? null : argsIter.next();

            if (strVal != null && strVal.startsWith(NAME_PREFIX))
                throw new IllegalArgumentException("Unexpected value: " + strVal);

            try {
                Object value = parseVal(strVal, cliArg.type());

                ((CLIArgument<Object>)cliArg).validator().accept(cliArg.name(), value);

                parsedArgs.put(cliArg.name(), value);
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Failed to parse " + cliArg.name() + " command argument. "
                    + e.getMessage());
            }

            obligatoryArgs.remove(cliArg.name());
        }

        if (!obligatoryArgs.isEmpty())
            throw new IllegalArgumentException("Mandatory argument(s) missing: " + obligatoryArgs);
    }

    /**
     * Get parsed argument value.
     *
     * @param arg Argument configuration.
     * @param <T> Value type.
     * @return Value.
     */
    public <T> T get(CLIArgument<?> arg) {
        Object val = parsedArgs.get(arg.name());

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
        CLIArgument<?> arg = argConfiguration.get(name);

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
        if (parsedPositionalArgs.size() - 1 < position)
            return null;

        return (T)parsedPositionalArgs.get(position);
    }

    /**
     * Returns usage description.
     *
     * @return Usage.
     */
    public String usage() {
        GridStringBuilder sb = new GridStringBuilder("Usage: ");

        for (CLIArgument<?> arg : argConfiguration.values())
            sb.a(argNameForUsage(arg)).a(" ");

        for (CLIArgument<?> arg : argConfiguration.values()) {
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

        return sb.toString();
    }

    /** */
    private String argNameForUsage(CLIArgument<?> arg) {
        if (arg.optional())
            return "[" + arg.name() + "]";
        else
            return arg.name();
    }
}
