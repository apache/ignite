/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 *
 */
public class VisorValidateIndexesTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Caches. */
    private Set<String> caches;

    /** Check first K elements. */
    private int checkFirst;

    /** Check through K element (skip K-1, check Kth). */
    private int checkThrough;

    /** Nodes on which task will run. */
    private Set<UUID> nodes;

    /**
     * Default constructor.
     */
    public VisorValidateIndexesTaskArg() {
        // No-op.
    }

    /**
     * @param caches Caches.
     */
    public VisorValidateIndexesTaskArg(Set<String> caches, Set<UUID> nodes, int checkFirst, int checkThrough) {
        this.caches = caches;
        this.checkFirst = checkFirst;
        this.checkThrough = checkThrough;
        this.nodes = nodes;
    }

    /**
     * @return Caches.
     */
    public Set<String> getCaches() {
        return caches;
    }

    /**
     * @return Nodes on which task will run. If {@code null}, task will run on all server nodes.
     */
    public Set<UUID> getNodes() {
        return nodes;
    }

    /**
     * @return checkFirst.
     */
    public int getCheckFirst() {
        return checkFirst;
    }

    /**
     * @return checkThrough.
     */
    public int getCheckThrough() {
        return checkThrough;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);
        out.writeInt(checkFirst);
        out.writeInt(checkThrough);
        U.writeCollection(out, nodes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        caches = U.readSet(in);

        if (protoVer > V1) {
            checkFirst = in.readInt();
            checkThrough = in.readInt();
        }
        else {
            checkFirst = -1;
            checkThrough = -1;
        }

        if (protoVer > V2)
            nodes = U.readSet(in);
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorValidateIndexesTaskArg.class, this);
    }
}
