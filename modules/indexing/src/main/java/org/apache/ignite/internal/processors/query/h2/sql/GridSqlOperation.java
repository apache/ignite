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

/**
 * Unary or binary operation.
 */
public class GridSqlOperation extends GridSqlElement {
    /** */
    private final GridSqlOperationType opType;

    /**
     * @param opType Operation type.
     */
    public GridSqlOperation(GridSqlOperationType opType) {
        this.opType = opType;
    }

    /**
     * @param opType Op type.
     * @param arg argument.
     */
    public GridSqlOperation(GridSqlOperationType opType, GridSqlElement arg) {
        this(opType);

        addChild(arg);
    }

    /**
     * @param opType Op type.
     * @param left Left.
     * @param right Right.
     */
    public GridSqlOperation(GridSqlOperationType opType, GridSqlElement left, GridSqlElement right) {
        this(opType);

        addChild(left);
        addChild(right);
    }

    /**
     * @return Left.
     */
    public GridSqlElement left() {
        return child(0);
    }

    /**
     * @return Right.
     */
    public GridSqlElement right() {
        return child(1);
    }

    /**
     * @return Operation type.
     */
    public GridSqlOperationType opType() {
        return opType;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return opType.toSql(this);
    }
}
