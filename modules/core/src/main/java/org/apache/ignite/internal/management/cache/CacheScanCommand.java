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

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.SystemViewTask;
import org.apache.ignite.internal.management.api.ComputeCommand;

/** Scan cache entries. */
public class CacheScanCommand implements ComputeCommand<CacheScanCommandArg, CacheScanTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Show cache content";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheScanCommandArg> argClass() {
        return CacheScanCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<CacheScanTask> taskClass() {
        return CacheScanTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(CacheScanCommandArg arg, CacheScanTaskResult res, Consumer<String> printer) {
        List<SystemViewTask.SimpleType> types = res.titles().stream()
            .map(x -> SystemViewTask.SimpleType.STRING).collect(Collectors.toList());

        SystemViewCommand.printTable(res.titles(), types, res.entries(), printer);

        if (res.entries().size() == arg.limit())
            printer.accept("Result limited to " + arg.limit() + " rows. Limit can be changed with '--limit' argument.");
    }
}
