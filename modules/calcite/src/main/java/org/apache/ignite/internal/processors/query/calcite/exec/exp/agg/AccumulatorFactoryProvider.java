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

import java.util.function.Supplier;
import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Factory that selects and creates an accumulator supplier for an aggregate call. Allows overriding standard aggregate
 * functions.
 *
 * <p>It can be set via {@link PluginProvider} when creating a configuration using
 * {@link PluginProvider#createComponent} via {@link Frameworks.ConfigBuilder#context(Context)}.</p>
 */
@FunctionalInterface
public interface AccumulatorFactoryProvider {
    /** @return Accumulator supplier, {@code null} if no accumulator is required for this aggregate call. */
    @Nullable <Row> Supplier<Accumulator<Row>> factory(AggregateCall call, ExecutionContext<Row> ctx);
}
