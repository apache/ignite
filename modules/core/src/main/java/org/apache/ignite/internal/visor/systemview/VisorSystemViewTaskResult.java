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

package org.apache.ignite.internal.visor.systemview;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Reperesents result of {@link VisorSystemViewTask}.
 */
public class VisorSystemViewTaskResult extends IgniteDataTransferObject {
    /** Attribute values for each row of the system view. */
    private List<? extends List<?>> rows;

    /** Names of the system view attributes. */
    private List<String> attrs;

    /** Types of the systen view attributes. */
    private List<Class<?>> types;

    /**
     * Default constructor.
     */
    public VisorSystemViewTaskResult() {
        // No-op.
    }

    /**
     * @param attrs Names of system view attributes.
     * @param rows Attribute values for each row of the system view.
     */
    public VisorSystemViewTaskResult(List<String> attrs, List<Class<?>> types, List<? extends List<?>> rows) {
        this.attrs = attrs;
        this.types = types;
        this.rows = rows;
    }

    /**
     * @return Names of the system view attributes.
     */
    public List<String> systemViewAttributes() {
        return attrs;
    }

    /**
     * @return Attribute values for each row of the system view.
     */
    public List<? extends List<?>> systemViewContent() {
        return rows;
    }

    /**
     * @return Types of the system view attributes.
     */
    public List<Class<?>> systemViewAttributeTypes() {
        return types;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, attrs);

        U.writeCollection(out, types);

        out.writeInt(rows.size());

        for (List<?> row : rows)
            U.writeCollection(out, row);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        attrs = U.readList(in);

        types = U.readList(in);

        int rowsCnt = in.readInt();

        List<List<Object>> rows = new ArrayList<>(rowsCnt);

        for (int i = 0; i < rowsCnt; i++)
            rows.add(U.readList(in));

        this.rows = rows;
    }
}
