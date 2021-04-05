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

import org.apache.calcite.rel.type.RelDataType;

public class ColumnDefinition {
    private String name;
    private RelDataType type;
    private Object dflt;
    private boolean nullable;

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }

    public RelDataType type() {
        return type;
    }

    public void type(RelDataType type) {
        this.type = type;
    }

    public Object defaultValue() {
        return dflt;
    }

    public void defaultValue(Object dflt) {
        this.dflt = dflt;
    }

    public boolean nullable() {
        return nullable;
    }

    public void nullable(boolean nullable) {
        this.nullable = nullable;
    }

    public Integer precision() {
        return type != null && type.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED ? type.getPrecision() : null;
    }

    public Integer scale() {
        return type != null && type.getScale() != RelDataType.SCALE_NOT_SPECIFIED ? type.getScale() : null;
    }
}
