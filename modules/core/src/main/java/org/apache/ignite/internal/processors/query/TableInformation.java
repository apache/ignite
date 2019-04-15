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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.util.Objects;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Information about table.
 */
public class TableInformation {
    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tblName;

    /** Table type. */
    private final String tblType;


    /**
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param tblType Table type.
     */
    public TableInformation(String schemaName, String tblName, String tblType) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.tblType = tblType;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Table type.
     */
    public String tableType() {
        return tblType;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TableInformation tblInfo = (TableInformation)o;

        return F.eq(schemaName, tblInfo.schemaName) && F.eq(tblName, tblInfo.tblName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(schemaName, tblName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TableInformation.class, this);
    }

}
