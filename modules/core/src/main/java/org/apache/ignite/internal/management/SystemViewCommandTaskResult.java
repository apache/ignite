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

package org.apache.ignite.internal.management;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.management.SystemViewCommandTask.SimpleType;

/** Reperesents result of {@link SystemViewCommandTask}. */
public class SystemViewCommandTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Attribute values for each row of the system view per node ID. */
    private Map<UUID, List<List<?>>> rows;

    /** Names of the system view attributes. */
    private List<String> attrs;

    /** Types of the system view attributes. */
    List<SimpleType> types;

    /** Default constructor. */
    public SystemViewCommandTaskResult() {
        // No-op.
    }

    /**
     * @param attrs Names of system view attributes.
     * @param types Types of the system view attributes.
     * @param rows Attribute values for each row of the system view per node ID.
     */
    public SystemViewCommandTaskResult(List<String> attrs, List<SimpleType> types, Map<UUID, List<List<?>>> rows) {
        this.attrs = attrs;
        this.types = types;
        this.rows = rows;
    }

    /** @return Names of the system view attributes. */
    public List<String> attributes() {
        return attrs;
    }

    /** @return Attribute values for each row of the system view per node ID. */
    public Map<UUID, List<List<?>>> rows() {
        return rows;
    }

    /** @return Types of the system view attributes. */
    public List<SimpleType> types() {
        return types;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, attrs);

        U.writeCollection(out, types);

        U.writeMap(out, rows);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        attrs = U.readList(in);

        types = U.readList(in);

        rows = U.readTreeMap(in);
    }
}
