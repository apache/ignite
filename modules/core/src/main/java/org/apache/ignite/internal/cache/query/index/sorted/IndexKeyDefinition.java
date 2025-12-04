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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.cache.query.index.IndexKeyTypeMessage;
import org.apache.ignite.internal.cache.query.index.OrderMessage;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Defines a signle index key.
 */
public class IndexKeyDefinition implements Message, Externalizable {
    /** */
    // TODO
    private static final long serialVersionUID = 0L;

    /** A message for {@link IndexKeyType}. */
    @org.apache.ignite.internal.Order(value = 0, method = "indexKeyTypeMessage")
    private IndexKeyTypeMessage idxTypeMsg;

    /** Order. */
    private OrderMessage order;

    /** Precision for variable length key types. */
    @org.apache.ignite.internal.Order(1)
    private int precision;

    /** */
    public IndexKeyDefinition() {
        // No-op.
    }

    /** */
    public IndexKeyDefinition(int idxTypeCode, OrderMessage order, long precision) {
        idxTypeMsg = new IndexKeyTypeMessage(idxTypeCode);
        this.order = order;

        // Workaround due to wrong type conversion (int -> long).
        if (precision >= Integer.MAX_VALUE)
            this.precision = -1;
        else
            this.precision = (int)precision;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 119;
    }

    /** */
    public OrderMessage order() {
        return order;
    }

    /** */
    public IndexKeyType idxType() {
        return idxTypeMsg.value();
    }

    /** */
    public int precision() {
        return precision;
    }

    /** */
    public void precision(int precision) {
        this.precision = precision;
    }

    /** */
    public IndexKeyTypeMessage indexKeyTypeMessage() {
        return idxTypeMsg;
    }

    /** */
    public void indexKeyTypeMessage(IndexKeyTypeMessage idxTypeMsg) {
        this.idxTypeMsg = idxTypeMsg;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // TODO
        assert false;

        // Send only required info for using in MergeSort algorithm.
        out.writeInt(idxTypeMsg.value().code());
        U.writeEnum(out, order.sortOrder());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO
        assert false;

        idxTypeMsg = new IndexKeyTypeMessage(in.readInt());
        order = new OrderMessage(U.readEnum(in, SortOrder.class), null);
    }
}
