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

package org.apache.ignite.internal.processors.rest.handlers.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Client query result.
 */
public class CacheQueryResult implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query ID. */
    private long qryId;

    /** Result items. */
    private Collection<?> items;

    /** Fields metadata. */
    private Collection<?> fieldsMeta;

    /** Last flag. */
    private boolean last;

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return qryId;
    }

    /**
     * @param qryId Query ID.
     */
    public void setQueryId(long qryId) {
        this.qryId = qryId;
    }

    /**
     * @return Items.
     */
    public Collection<?> getItems() {
        return items;
    }

    /**
     * @param items Items.
     */
    public void setItems(Collection<?> items) {
        this.items = items;
    }

    /**
     * @param fieldsMeta Fields metadata.
     */
    public void setFieldsMetadata(Collection<?> fieldsMeta) {
        this.fieldsMeta = fieldsMeta;
    }

    /**
     * @return Fields metadata.
     */
    public Collection<?> getFieldsMetadata() {
        return fieldsMeta;
    }

    /**
     * @return Last flag.
     */
    public boolean getLast() {
        return last;
    }

    /**
     * @param last Last flag.
     */
    public void setLast(boolean last) {
        this.last = last;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheQueryResult.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(last);
        out.writeLong(qryId);
        U.writeCollection(out, items);
        U.writeCollection(out, fieldsMeta);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        last = in.readBoolean();
        qryId = in.readLong();
        items = U.readCollection(in);
        fieldsMeta = U.readCollection(in);
    }
}