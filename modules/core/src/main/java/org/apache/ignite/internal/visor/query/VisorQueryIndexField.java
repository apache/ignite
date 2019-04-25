/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for {@link QueryEntity}.
 */
public class VisorQueryIndexField extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index field name. */
    private String name;

    /** Index field sort order. */
    private boolean sort;

    /**
     * Create data transfer object for given cache type metadata.
     */
    public VisorQueryIndexField() {
        // No-op.
    }

    /**
     * Create data transfer object for index field.
     *
     * @param name Index field name.
     * @param sort Index field sort order.
     */
    public VisorQueryIndexField(String name, boolean sort) {
        this.name = name;
        this.sort = sort;
    }

    /**
     * @param idx Query entity index.
     * @return Data transfer object for query entity index fields.
     */
    public static List<VisorQueryIndexField> list(QueryIndex idx) {
        List<VisorQueryIndexField> res = new ArrayList<>();

        for (Map.Entry<String, Boolean> field: idx.getFields().entrySet())
            res.add(new VisorQueryIndexField(field.getKey(), !field.getValue()));

        return res;
    }

    /**
     * @return Index field name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Index field sort order.
     */
    public boolean getSortOrder() {
        return sort;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        out.writeBoolean(sort);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        sort = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryIndexField.class, this);
    }
}
