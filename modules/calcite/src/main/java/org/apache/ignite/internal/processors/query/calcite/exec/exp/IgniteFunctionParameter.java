/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

/** Function parameter that exposes its SQL type to validation. */
class IgniteFunctionParameter implements FunctionParameter {
    /** */
    private final FunctionParameter delegate;

    /** */
    IgniteFunctionParameter(FunctionParameter delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public int getOrdinal() {
        return delegate.getOrdinal();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return delegate.getName();
    }

    /** {@inheritDoc} */
    @Override public RelDataType getType(RelDataTypeFactory typeFactory) {
        // Normalize UDF metadata without losing Java types used to convert query results.
        return ((JavaTypeFactory)typeFactory).toSql(delegate.getType(typeFactory));
    }

    /** {@inheritDoc} */
    @Override public boolean isOptional() {
        return delegate.isOptional();
    }
}
