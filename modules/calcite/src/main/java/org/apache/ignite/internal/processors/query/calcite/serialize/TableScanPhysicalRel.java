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

    /** Row type after the project. */
    private DataType rowType;

    /** */
    private Expression cond;

    /** */
    private int[] projects;

    /** */
    public TableScanPhysicalRel() {
    }

    /**
     * @param tblName Qualified table name
     */
    public TableScanPhysicalRel(List<String> tblName) {
        this.tblName = tblName;
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
    public Expression condition() {
        return cond;
    }

    /**
     * @return Projection.
     */
    public int[] projects() {
        return projects;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(rowType);
        out.writeObject(cond);

        out.writeInt(tblName.size());
        for (String name : tblName)
            out.writeUTF(name);

        if (projects == null) {
            out.writeInt(0);
        }
        else {
            out.writeInt(projects.length);
            for (int i = 0; i < projects.length; i++)
                out.writeInt(projects[i]);
        }

    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowType = (DataType) in.readObject();
        cond = (Expression) in.readObject();

        int tblNameSize = in.readInt();
        List<String> tblName = new ArrayList<>(tblNameSize);
        for (int i = 0; i < tblNameSize; i++)
            tblName.add(in.readUTF());
        this.tblName = tblName;

        int projectsSize = in.readInt();
        if (projectsSize > 0) {
            projects = new int[projectsSize];
            for (int i = 0; i < projectsSize; i++)
                projects[i] = in.readInt();
        }
    }
}
