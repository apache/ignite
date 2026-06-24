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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.agg;

import java.util.Map;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.plugin.Extension;
import org.apache.ignite.plugin.PluginProvider;

/** Class for extending {@link Accumulators} via {@link PluginProvider plugins}. */
@FunctionalInterface
public interface PluginAccumulatorsExtension extends Extension {
    /** @return Accumulator factories by aggregate function name. Name must be non-empty and unique. */
    Map<String, PluginAccumulatorFactory<?>> accumulatorFactories();

    /** */
    @FunctionalInterface
    interface PluginAccumulatorFactory<Row> {
        /** */
        Accumulator<Row> create(AggregateCall call, ExecutionContext<Row> ctx);
    }
}
