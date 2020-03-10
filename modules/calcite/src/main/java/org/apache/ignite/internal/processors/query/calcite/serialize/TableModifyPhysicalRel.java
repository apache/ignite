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
import org.apache.calcite.rel.core.TableModify;

/**
 *
 */
public class TableModifyPhysicalRel implements PhysicalRel {
    /** */
    private List<String> tableName;

    /** */
    private TableModify.Operation operation;

    /** */
    private List<String> updateColumnList;

    /** */
    private PhysicalRel input;

    public TableModifyPhysicalRel() {
    }

    /***/
    public TableModifyPhysicalRel(List<String> tableName, TableModify.Operation operation, List<String> updateColumnList, PhysicalRel input) {
        this.tableName = tableName;
        this.operation = operation;
        this.updateColumnList = updateColumnList;
        this.input = input;
    }

    public List<String> tableName() {
        return tableName;
    }

    public TableModify.Operation operation() {
        return operation;
    }

    public List<String> updateColumnList() {
        return updateColumnList;
    }

    public PhysicalRel input() {
        return input;
    }

    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(tableName.size());

        for (String name : tableName)
            out.writeUTF(name);

        out.writeByte(operation.ordinal());

        if (updateColumnList == null)
            out.writeInt(-1);
        else {
            out.writeInt(updateColumnList.size());

            for (String column : updateColumnList)
                out.writeUTF(column);
        }

        out.writeObject(input);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int tableNameSize = in.readInt();

        List<String> tableName = new ArrayList<>(tableNameSize);

        for (int i = 0; i < tableNameSize; i++)
            tableName.add(in.readUTF());

        operation = TableModify.Operation.values()[in.readByte()];

        int columnsSize = in.readInt();

        if (columnsSize != -1) {
            List<String> updateColumnList = new ArrayList<>(columnsSize);

            for (int i = 0; i < columnsSize; i++)
                updateColumnList.add(in.readUTF());
        }

        input = (PhysicalRel) in.readObject();
    }
}
