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

package org.apache.ignite.internal.visor.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for {@link QueryIndex}.
 */
public class VisorQueryIndex extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name of index. */
    private String name;

    /** Type of index. */
    private QueryIndexType type;

    /** Fields to create group indexes for. */
    private List<VisorQueryIndexField> fields;

    /**
     * Create data transfer object for given cache type metadata.
     */
    public VisorQueryIndex() {
        // No-op.
    }

    /**
     * Create data transfer object for given cache type metadata.
     *
     * @param idx Actual cache query entity index.
     */
    public VisorQueryIndex(QueryIndex idx) {
        assert idx != null;

        name = idx.getName();
        type = idx.getIndexType();
        fields = VisorQueryIndexField.list(idx);
    }

    /**
     * @return Name of index.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Type of index.
     */
    public QueryIndexType getType() {
        return type;
    }

    /**
     * @return Fields to create group indexes for.
     */
    public List<VisorQueryIndexField> getFields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeEnum(out, type);
        U.writeCollection(out, fields);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        type = QueryIndexType.fromOrdinal(in.readByte());
        fields = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryIndex.class, this);
    }
}
