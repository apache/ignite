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
package org.apache.ignite.internal.processors.query.calcite.prepare.ddl;

import java.util.Objects;

import org.apache.calcite.rel.type.RelDataType;
import org.jetbrains.annotations.Nullable;

/** Definies a particular column within table. */
public class ColumnDefinition {
    /** */
    private final String name;

    /** */
    private final RelDataType type;

    /** */
    private final Object dflt;

    /** Creates a column definition. */
    public ColumnDefinition(String name, RelDataType type, @Nullable Object dflt) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.dflt = dflt;
    }

    /**
     * @return Column's name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Column's type.
     */
    public RelDataType type() {
        return type;
    }

    /**
     * @return Column's default value.
     */
    public @Nullable Object defaultValue() {
        return dflt;
    }

    /**
     * @return {@code true} if this column accepts nulls.
     */
    public boolean nullable() {
        return type.isNullable();
    }

    /**
     * @return Column's precision.
     */
    public Integer precision() {
        return type.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED ? type.getPrecision() : null;
    }

    /**
     * @return Column's scale.
     */
    public Integer scale() {
        return type.getScale() != RelDataType.SCALE_NOT_SPECIFIED ? type.getScale() : null;
    }
}
