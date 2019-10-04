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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryUtils;

/**
 *
 */
public final class Commons {
    private Commons(){}

    public static Context convert(QueryContext ctx) {
        return ctx == null ? Contexts.empty() : Contexts.of(ctx.unwrap(Object[].class));
    }

    public static <T> Predicate<T> any() {
        return obj -> true;
    }

    /** */
    public static Function<RelDataTypeFactory, RelDataType> rowTypeFunction(GridQueryTypeDescriptor desc) {
        return (f) -> {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(f);

            builder.add(QueryUtils.KEY_FIELD_NAME, f.createJavaType(desc.keyClass()));
            builder.add(QueryUtils.VAL_FIELD_NAME, f.createJavaType(desc.valueClass()));

            for (Map.Entry<String, Class<?>> prop : desc.fields().entrySet()) {
                builder.add(prop.getKey(), f.createJavaType(prop.getValue()));
            }
            return builder.build();
        };
    }

    public static boolean transformSubset(RelOptRuleCall call, RelTraitSet traits) {

        return false;
    }
}
