/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.commandline.argument.parser;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Command line argument.
 * @param <T> Value type.
 */
public class CLIArgument<T> {
    /** */
    private final String name;

    /** */
    private final String usage;

    /** */
    private final boolean isOptional;

    /** */
    private final Class<T> type;

    /** */
    private final Function<CLIArgumentParser, T> dfltValSupplier;

    /** */
    public static <T> CLIArgument<T> optionalArg(String name, String usage, Class<T> type) {
        return new CLIArgument<T>(name, usage, true, type, null);
    }

    /** */
    public static <T> CLIArgument<T> optionalArg(String name, String usage, Class<T> type, Supplier<T> dfltValSupplier) {
        return new CLIArgument<T>(name, usage, true, type, p -> dfltValSupplier.get());
    }

    /** */
    public static <T> CLIArgument<T> optionalArg(String name, String usage, Class<T> type, Function<CLIArgumentParser, T> dfltValSupplier) {
        return new CLIArgument<T>(name, usage, true, type, dfltValSupplier);
    }

    /** */
    public static <T> CLIArgument<T> mandatoryArg(String name, String usage, Class<T> type) {
        return new CLIArgument<T>(name, usage, false, type, null);
    }

    /** */
    public CLIArgument(String name, String usage, boolean isOptional, Class<T> type, Function<CLIArgumentParser, T> dfltValSupplier) {
        this.name = name;
        this.usage = usage;
        this.isOptional = isOptional;
        this.type = type;
        this.dfltValSupplier = dfltValSupplier == null
            ? (type.equals(Boolean.class) ? p -> (T) Boolean.FALSE : p -> null)
            : dfltValSupplier;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String usage() {
        return usage;
    }

    /** */
    public boolean optional() {
        return isOptional;
    }

    /** */
    public Class type() {
        return type;
    }

    /** */
    public Function<CLIArgumentParser, T> defaultValueSupplier() {
        return dfltValSupplier;
    }
}
