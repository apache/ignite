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
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Command line argument.
 * @param <T> Value type.
 */
public class CLIArgument<T> {
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
    private final boolean isSensitive;

    /** */
    private CLIArgument(
        String name,
        Class<T> type,
        String usage,
        Function<CLIArgumentParser, T> dfltValSupplier,
        BiConsumer<String, T> validator,
        boolean isOptional,
        boolean isSensitive
    ) {
        this.name = name;
        this.type = type;
        this.usage = usage;
        this.dfltValSupplier = dfltValSupplier;
        this.validator = validator;
        this.isOptional = isOptional;
        this.isSensitive = isSensitive;
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
        return validator;
    }

    /** */
    public boolean optional() {
        return isOptional;
    }

    /** */
    public boolean isSensitive() {
        return isSensitive;
    }

    /** */
    public boolean isFlag() {
        return type.equals(Boolean.class) || type.equals(boolean.class);
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
        private boolean isSensitive;

        /** */
        private CLIArgumentBuilder(String name, Class<T> type, boolean isOptional) {
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
        public CLIArgumentBuilder<T> withDefault(T dflt) {
            dfltValSupplier = t -> dflt;

            return this;
        }

        /** */
        public CLIArgumentBuilder<T> withDefault(Function<CLIArgumentParser, T> dfltValSupplier) {
            this.dfltValSupplier = dfltValSupplier;

            return this;
        }

        /** */
        public CLIArgumentBuilder<T> withValidator(BiConsumer<String, T> validator) {
            this.validator = validator;

            return this;
        }

        /** */
        public CLIArgumentBuilder<T> markOptional() {
            isOptional = true;

            return this;
        }

        /** */
        public CLIArgumentBuilder<T> withOptional(boolean optional) {
            isOptional = optional;

            return this;
        }

        /** */
        public CLIArgumentBuilder<T> markSensitive() {
            isSensitive = true;

            return this;
        }

        /** */
        public CLIArgumentBuilder<T> withSensitive(boolean sensitive) {
            isSensitive = sensitive;

            return this;
        }

        /**  */
        public static <T> CLIArgumentBuilder<T> argument(String name, Class<T> type) {
            return new CLIArgumentBuilder<>(name, type, false);
        }

        /** */
        public static <T> CLIArgumentBuilder<T> optionalArgument(String name, Class<T> type) {
            return new CLIArgumentBuilder<>(name, type, true);
        }

        /** */
        public CLIArgument<T> build() {
            A.notNull(name, "name");
            A.notNull(type, "type");

            boolean isFlag = type.equals(Boolean.class) || type.equals(boolean.class);

            if (isFlag && isSensitive)
                throw new IllegalArgumentException("Flag argument can't be sensitive");

            Function<CLIArgumentParser, T> dfltValSupplier = this.dfltValSupplier;

            if (dfltValSupplier == null)
                dfltValSupplier = isFlag ? p -> (T)Boolean.FALSE : p -> null;

            BiConsumer<String, T> validator = this.validator;

            if (validator == null)
                validator = (name, val) -> {};

            return new CLIArgument<>(name, type, usage, dfltValSupplier, validator, isOptional, isSensitive);
        }
    }
}
