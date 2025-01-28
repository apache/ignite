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

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Command line argument.
 * @param <T> Value type.
 */
public class CLIArgument<T> {
    /** */
    private final BiConsumer<String, T> EMPTY = (name, val) -> {};

    /** */
    private final String name;

    /** */
    private final Class<T> type;

    /** */
    private final String usage;

    /** */
    private final Function<CLIArgumentParser, T> dfltValSupplier;

    /** */
    private final BiConsumer<String, T> validator;

    /** */
    private final boolean isOptional;

    /** */
    private final boolean isInteractive;

    /** */
    private CLIArgument(
        CLIArgumentBuilder<T> builder
    ) {
        name = builder.name;
        type = builder.type;
        usage = builder.usage;
        dfltValSupplier = builder.dfltValSupplier;
        validator = builder.validator;
        isOptional = builder.isOptional;
        isInteractive = builder.isInteractive;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public Class<T> type() {
        return type;
    }

    /** */
    public String usage() {
        return usage;
    }

    /** */
    public Function<CLIArgumentParser, T> defaultValueSupplier() {
        return dfltValSupplier;
    }

    /** */
    public BiConsumer<String, T> validator() {
        return validator == null ? EMPTY : validator;
    }

    /** */
    public boolean optional() {
        return isOptional;
    }

    /** */
    public boolean isInteractive() {
        return isInteractive;
    }

    /** */
    public boolean isFlag() {
        return type.equals(Boolean.class) || type.equals(boolean.class);
    }

    /**  */
    public static CLIArgumentBuilder<?> argument(String name, Class<?> type) {
        return new CLIArgumentBuilder<>(name, type, false);
    }

    /** */
    public static CLIArgumentBuilder<?> optionalArgument(String name, Class<?> type) {
        return new CLIArgumentBuilder<>(name, type, true);
    }

    /**
     * Command line argument builder.
     * @param <T> Value type.
     */
    public static class CLIArgumentBuilder<T> {
        /** */
        private final String name;

        /** */
        private final Class<T> type;

        /** */
        private String usage;

        /** */
        private Function<CLIArgumentParser, T> dfltValSupplier;

        /** */
        private BiConsumer<String, T> validator;

        /** */
        private boolean isOptional;

        /** */
        private boolean isInteractive;

        /** */
        public CLIArgumentBuilder(String name, Class<T> type, boolean isOptional) {
            this.name = name;
            this.type = type;
            this.isOptional = isOptional;
        }

        /** */
        public CLIArgumentBuilder<T> withUsage(String usage) {
            this.usage = usage;
            return this;
        }

        /** */
        public CLIArgumentBuilder<T> withDefault(Object dflt) {
            dfltValSupplier = t -> (T)dflt;
            return this;
        }

        /** */
        public CLIArgumentBuilder<T> withValidator(BiConsumer<String, ?> validator) {
            this.validator = (BiConsumer<String, T>)validator;
            return this;
        }

        /** */
        public CLIArgumentBuilder<T> markOptional() {
            isOptional = true;
            return this;
        }

        /** */
        public CLIArgumentBuilder<T> setOptional(boolean optional) {
            isOptional = optional;
            return this;
        }

        /** */
        public CLIArgumentBuilder<T> markInteractive() {
            isInteractive = true;
            return this;
        }

        /** */
        public CLIArgumentBuilder<T> setInteractive(boolean interactive) {
            isInteractive = interactive;
            return this;
        }

        /** */
        public CLIArgument<T> build() {
            assert name != null;
            assert type != null;

            if (isFlag() && isInteractive)
                throw new IllegalArgumentException("Flag argument can't be interactive");

            if (dfltValSupplier == null)
                dfltValSupplier = isFlag() ? p -> (T)Boolean.FALSE : p -> null;

            return new CLIArgument<>(this);
        }

        /** */
        private boolean isFlag() {
            return type.equals(Boolean.class) || type.equals(boolean.class);
        }
    }
}
