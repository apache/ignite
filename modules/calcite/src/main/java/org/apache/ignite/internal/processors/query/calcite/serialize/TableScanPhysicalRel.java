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

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.Expression;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;

/**
 * Describes {@link IgniteTableScan}.
 */
public class TableScanPhysicalRel implements PhysicalRel {
    /** */
    private List<String> tblName;

    /** */
    private String idxName;

    /** Row type after the project. */
    private DataType rowType;

    /** */
    private List<Expression> filters;

    /** */
    private List<Expression> lowerBound;

    /** */
    private List<Expression> upperBound;

    /** */
    public TableScanPhysicalRel() {
    }

    /**
     * @param tblName Qualified table name
     */
    public TableScanPhysicalRel(
        List<String> tblName,
        String idxName,
        DataType rowType,
        List<Expression> filters,
        List<Expression> lowerBound,
        List<Expression> upperBound) {
        this.tblName = tblName;
        this.idxName = idxName;
        this.rowType = rowType;
        this.filters = filters;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    /**
     * @return Full table name.
     */
    public List<String> tableName() {
        return tblName;
    }

    /**
     * @return Row type.
     */
    public DataType rowType() {
        return rowType;
    }

    /**
     * @return Filter confition.
     */
    public List<Expression> filters() {
        return filters;
    }

    /**
     * @return Lower index scan bound.
     */
    public List<Expression> lowerBound() {
        return lowerBound;
    }

    /**
     * @return Upper index scan bound.
     */
    public List<Expression> upperBound() {
        return upperBound;
    }

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(rowType);
        out.writeUTF(idxName);

        out.writeInt(tblName.size());
        for (String name : tblName)
            out.writeUTF(name);

        if (filters == null)
            out.writeInt(0);
        else {
            out.writeInt(filters.size());
            for (int i = 0; i < filters.size(); i++)
                out.writeObject(filters.get(i));
        }

        if (lowerBound == null)
            out.writeInt(0);
        else {
            out.writeInt(lowerBound.size());
            for (int i = 0; i < lowerBound.size(); i++)
                out.writeObject(lowerBound.get(i));
        }

        if (upperBound == null)
            out.writeInt(0);
        else {
            out.writeInt(upperBound.size());
            for (int i = 0; i < upperBound.size(); i++)
                out.writeObject(upperBound.get(i));
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowType = (DataType) in.readObject();
        idxName = in.readUTF();

        int tblNameSize = in.readInt();
        List<String> tblName = new ArrayList<>(tblNameSize);
        for (int i = 0; i < tblNameSize; i++)
            tblName.add(in.readUTF());
        this.tblName = tblName;

        int filtersSize = in.readInt();
        if (filtersSize > 0) {
            filters = new ArrayList<>(filtersSize);
            for (int i = 0; i < filtersSize; i++)
                filters.add((Expression)in.readObject());
        }

        int lowerBoundSize = in.readInt();
        if (lowerBoundSize > 0) {
            lowerBound = new ArrayList<>(lowerBoundSize);
            for (int i = 0; i < lowerBoundSize; i++)
                lowerBound.add((Expression)in.readObject());
        }

        int upperBoundSize = in.readInt();
        if (upperBoundSize > 0) {
            upperBound = new ArrayList<>(upperBoundSize);
            for (int i = 0; i < upperBoundSize; i++)
                upperBound.add((Expression)in.readObject());
        }
    }
}
