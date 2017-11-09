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

package org.apache.ignite.internal.visor.baseline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Datatransfer object for {@link BaselineNode}.
 */
public class VisorBaselineNode extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String consistentId;

    /** */
    private Map<String, Object> attrs;

    /** */
    private boolean alive;

    /**
     * Default constructor.
     */
    public VisorBaselineNode() {
        // No-op.
    }

    /**
     * Create data transfer object for baseline node.
     *
     * @param node Baseline node.
     */
    public VisorBaselineNode(BaselineNode node) {
        consistentId = String.valueOf(node.consistentId());
        attrs = node.attributes();
        alive = true; // TODO WC-251 node.alive();
    }

    /**
     * @return Node consistent ID.
     */
    public String consistentId() {
        return consistentId;
    }

    /**
     * @return Node attributes.
     */
    public Map<String, Object> attributes() {
        return attrs;
    }

    /**
     * @return {@code true} If node is alive.
     */
    public boolean isAlive() {
        return alive;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, consistentId);
        U.writeMap(out, attrs);
        out.writeBoolean(alive);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        consistentId = U.readString(in);
        attrs = U.readMap(in);
        alive = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBaselineNode.class, this);
    }
}
