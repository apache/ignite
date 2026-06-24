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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.Extension;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;

/** Registry for {@link AccumulatorFactory}s. */
public class PluginAccumulatorFactoryRegistry {
    /** Factory by aggregate function name. */
    private final Map<String, AccumulatorFactory<?>> factoryByAggFunName;

    /** */
    public PluginAccumulatorFactoryRegistry(GridKernalContext ctx) {
        factoryByAggFunName = factories(ctx);
    }

    /** @return Plugin accumulator factory by aggregate function name or {@code null} if not found. */
    public @Nullable <Row> AccumulatorFactory<Row> factory(String aggFunName) {
        return (AccumulatorFactory<Row>) factoryByAggFunName.get(aggFunName);
    }

    /** Extension for getting {@link AccumulatorFactory} from {@link PluginProvider}. */
    @FunctionalInterface
    public interface PluginAccumulatorFactoryExtension extends Extension {
        /**
         * @return Accumulator factories by aggregate function name. Name must be non-empty, unique, and not reserved.
         */
        Map<String, AccumulatorFactory<?>> factories();
    }

    /** */
    private static Map<String, AccumulatorFactory<?>> factories(GridKernalContext ctx) {
        PluginAccumulatorFactoryExtension[] extensions = ctx.plugins().extensions(
            PluginAccumulatorFactoryExtension.class
        );

        if (F.isEmpty(extensions))
            return Map.of();

        Map<String, AccumulatorFactory<?>> res = new HashMap<>();

        for (PluginAccumulatorFactoryExtension extension : extensions) {
            for (Map.Entry<String, AccumulatorFactory<?>> e : extension.factories().entrySet()) {
                String aggFunName = e.getKey();

                if (aggFunName.isBlank())
                    throw new AssertionError("Invalid aggregate function name: " + aggFunName);
                else if (Accumulators.BUILT_IN_AGGREGATE_NAMES.contains(aggFunName))
                    throw new AssertionError("Aggregate function name is reserved: " + aggFunName);

                AccumulatorFactory<?> prev = res.putIfAbsent(aggFunName, e.getValue());

                if (prev != null)
                    throw new AssertionError("Duplicate aggregate function name: " + aggFunName);
            }
        }

        return Map.copyOf(res);
    }
}
