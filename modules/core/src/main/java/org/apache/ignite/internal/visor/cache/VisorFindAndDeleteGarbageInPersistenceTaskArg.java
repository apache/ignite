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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * {@link VisorFindAndDeleteGarbageInPersistenceTask} arguments.
 */
public class VisorFindAndDeleteGarbageInPersistenceTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Group names. */
    private Set<String> grpNames;

    /** Would be used  */
    private boolean deleteFoundGarbage;

    /** Nodes on which task will run. */
    private Set<UUID> nodes;

    /**
     * Default constructor.
     */
    public VisorFindAndDeleteGarbageInPersistenceTaskArg() {
        // No-op.
    }

    /**
     * @param grpNames Cache group names.
     */
    public VisorFindAndDeleteGarbageInPersistenceTaskArg(Set<String> grpNames, boolean deleteFoundGarbage, Set<UUID> nodes) {
        this.grpNames = grpNames;
        this.deleteFoundGarbage = deleteFoundGarbage;
        this.nodes = nodes;
    }

    /**
     * @return Caches.
     */
    public Set<String> getGrpNames() {
        return grpNames;
    }

    /**
     * @return Nodes on which task will run. If {@code null}, task will run on all server nodes.
     */
    public Set<UUID> getNodes() {
        return nodes;
    }

    /**
     * @return True if need to delete garbage as it found.
     */
    public boolean deleteFoundGarbage() {
        return deleteFoundGarbage;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, grpNames);
        out.writeBoolean(deleteFoundGarbage);
        U.writeCollection(out, nodes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        grpNames = U.readSet(in);
        deleteFoundGarbage = in.readBoolean();

        nodes = U.readSet(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorFindAndDeleteGarbageInPersistenceTaskArg.class, this);
    }
}
