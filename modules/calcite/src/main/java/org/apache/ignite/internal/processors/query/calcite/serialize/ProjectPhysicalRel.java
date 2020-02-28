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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;

/**
 * Describes {@link IgniteProject}.
 */
public class ProjectPhysicalRel implements PhysicalRel {
    /** */
    private DataType rowType;

    /** */
    private List<Expression> projects;

    /** */
    private PhysicalRel input;

    public ProjectPhysicalRel() {
    }

    /** */
    public ProjectPhysicalRel(DataType rowType, List<Expression> projects, PhysicalRel input) {
        this.rowType = rowType;
        this.projects = projects;
        this.input = input;
    }

    /** */
    public DataType rowType() {
        return rowType;
    }

    /** */
    public List<Expression> projects() {
        return projects;
    }

    /** */
    public PhysicalRel input() {
        return input;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(rowType);

        out.writeInt(projects.size());

        for (Expression project : projects)
            out.writeObject(project);

        out.writeObject(input);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowType = (DataType) in.readObject();

        int projectsSize = in.readInt();

        List<Expression> projects = new ArrayList<>(projectsSize);

        for (int i = 0; i < projectsSize; i++)
            projects.add((Expression) in.readObject());

        this.projects = projects;

        input = (PhysicalRel) in.readObject();
    }
}
