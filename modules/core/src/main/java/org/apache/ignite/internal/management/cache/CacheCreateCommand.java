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

package org.apache.ignite.internal.management.cache;

import java.util.Set;
import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/** Create caches. */
public class CacheCreateCommand implements ComputeCommand<CacheCreateCommandArg, Set<String>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Create caches from Spring XML configuration. Note that the '" + SPRING.module() + "' module should be enabled. " +
            "Cache filtering options configure the set of caches that will be processed by cache command. " +
            "Default value for --exclude-caches is empty set. ";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheCreateCommandArg> argClass() {
        return CacheCreateCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<CacheCreateTask> taskClass() {
        return CacheCreateTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(CacheCreateCommandArg arg, Set<String> res, Consumer<String> printer) {
        logParsedArgs(arg, printer);

        printer.accept(res.isEmpty() ? "No cache was created" : "Created caches: " + res);
    }

    /**
     * Passes parsed arguments to given log consumer.
     *
     * @param arg arguments.
     * @param logConsumer Logger.
     */
    public static void logParsedArgs(CacheCreateCommandArg arg, Consumer<String> logConsumer) {
        SB options = new SB("The procedure task was executed with the following args: ");

        options
            .a("excluded=[")
            .a(arg.excludeCaches() == null ? "" : String.join(", ", arg.excludeCaches()))
            .a("], springxmlConfig=[")
            .a(arg.springxmlconfig() == null ? "" : String.join(", ", arg.springxmlconfig()))
            .a("]\n");

        logConsumer.accept(options.toString());
    }
}
