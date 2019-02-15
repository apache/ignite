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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.jetbrains.annotations.Nullable;

/**
 * Map queyr column. Required to get proper data types for reduce table.
 */
public class MapColumn {
    /** No parameter. */
    private static final int PARAM_NONE = -1;

    /** Type. */
    private final GridSqlType typ;

    /** Parameter index. */
    private final int paramIdx;

    /**
     * Constructor for column with known type.
     *
     * @param typ Column type.
     */
    public MapColumn(GridSqlType typ) {
        this(typ, PARAM_NONE);
    }

    /**
     * Constructor for column with type defined by parameter.
     *
     * @param paramIdx Parameter index.
     */
    public MapColumn(int paramIdx) {
        this(null, paramIdx);
    }

    /**
     * Constructor.
     *
     * @param typ Column type.
     * @param paramIdx Parameter index.
     */
    private MapColumn(@Nullable GridSqlType typ, int paramIdx) {
        this.typ = typ;
        this.paramIdx = paramIdx;
    }

    /**
     * @return Type.
     */
    @Nullable public GridSqlType type() {
        return typ;
    }

    /**
     * @return Parameter index.
     */
    public int parameterIndex() {
        return paramIdx;
    }

    /**
     * @return Whether this is a parameter.
     */
    public boolean isParameter() {
        return paramIdx != PARAM_NONE;
    }
}
