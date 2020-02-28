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

/**
 *
 */
public class ValuesPhysicalRel implements PhysicalRel {
    /** */
    private List<Expression> values;

    /** */
    private int rowLen;

    public ValuesPhysicalRel() {
    }

    /** */
    public ValuesPhysicalRel(List<Expression> values, int rowLen) {
        this.values = values;
        this.rowLen = rowLen;
    }

    public List<Expression> values() {
        return values;
    }

    public int rowLength() {
        return rowLen;
    }

    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rowLen);

        out.writeInt(values.size());

        for (Expression value : values)
            out.writeObject(value);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowLen = in.readInt();

        int valuesSize = in.readInt();

        List<Expression> values = new ArrayList<>(valuesSize);

        for (int i = 0; i < valuesSize; i++)
            values.add((Expression) in.readObject());

        this.values = values;
    }
}
