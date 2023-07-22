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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.ComputeCommand;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.management.SystemViewCommand.printTable;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.STRING;

/** Enable / disable cache metrics collection or show metrics collection status. */
public class CacheMetricsCommand implements ComputeCommand<CacheMetricsCommandArg, CacheMetricsTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Manages user cache metrics collection: enables, disables it or shows status";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheMetricsCommandArg> argClass() {
        return CacheMetricsCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<CacheMetricsTask> taskClass() {
        return CacheMetricsTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(CacheMetricsCommandArg arg, CacheMetricsTaskResult res, Consumer<String> printer) {
        try {
            List<List<?>> values = res.result().entrySet()
                .stream()
                .map(e -> asList(e.getKey(), e.getValue() ? "enabled" : "disabled"))
                .collect(Collectors.toList());

            printTable(asList("Cache Name", "Metrics Status"), asList(STRING, STRING), values, printer);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }
}
