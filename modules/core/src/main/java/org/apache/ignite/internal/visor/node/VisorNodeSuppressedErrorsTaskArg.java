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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for task {@link VisorNodeSuppressedErrorsTask}
 */
public class VisorNodeSuppressedErrorsTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Last laded error orders. */
    private Map<UUID, Long> orders;

    /**
     * Default constructor.
     */
    public VisorNodeSuppressedErrorsTaskArg() {
        // No-op.
    }

    /**
     * @param orders Last laded error orders.
     */
    public VisorNodeSuppressedErrorsTaskArg(Map<UUID, Long> orders) {
        this.orders = orders;
    }

    /**
     * @return Last laded error orders.
     */
    public Map<UUID, Long> getOrders() {
        return orders;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, orders);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        orders = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeSuppressedErrorsTaskArg.class, this);
    }
}
