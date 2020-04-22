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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.util.IgniteUtils.readSet;
import static org.apache.ignite.internal.util.IgniteUtils.writeCollection;

/**
 *
 */
public class VisorValidateIndexesTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Caches. */
    private Set<String> caches;

    /** Check first K elements. */
    private int checkFirst = -1;

    /** Check through K element (skip K-1, check Kth). */
    private int checkThrough = -1;

    /** Nodes on which task will run. */
    private Set<UUID> nodes;

    /** Check CRC. */
    private boolean checkCrc;

    /** Check that index size and cache size are same. */
    private boolean checkSizes;

    /**
     * Default constructor.
     */
    public VisorValidateIndexesTaskArg() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param caches Caches.
     * @param nodes Nodes on which task will run.
     * @param checkFirst Check first K elements.
     * @param checkThrough Check through K element.
     * @param checkCrc Check CRC.
     * @param checkSizes Check that index size and cache size are same.
     */
    public VisorValidateIndexesTaskArg(
        Set<String> caches,
        Set<UUID> nodes,
        int checkFirst,
        int checkThrough,
        boolean checkCrc,
        boolean checkSizes
    ) {
        this.caches = caches;
        this.checkFirst = checkFirst;
        this.checkThrough = checkThrough;
        this.nodes = nodes;
        this.checkCrc = checkCrc;
        this.checkSizes = checkSizes;
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

    /**
     * @return checkCrc.
     */
    public boolean сheckCrc() {
        return checkCrc;
    }

    /**
     * Returns whether to check that index size and cache size are same.
     *
     * @return {@code true} if need check that index size and cache size
     *      are same.
     */
    public boolean checkSizes() {
        return checkSizes;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        writeCollection(out, caches);
        out.writeInt(checkFirst);
        out.writeInt(checkThrough);
        writeCollection(out, nodes);
        out.writeBoolean(checkCrc);
        out.writeBoolean(checkSizes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        caches = readSet(in);
        checkFirst = in.readInt();
        checkThrough = in.readInt();
        nodes = readSet(in);
        checkCrc = in.readBoolean();
        checkSizes = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorValidateIndexesTaskArg.class, this);
    }
}
