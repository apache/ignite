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

package org.apache.ignite.internal.visor.debug;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.baseline.VisorBaselineOperation;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;

/**
 * Argument for {@link VisorBaselineTask}.
 */
public class VisorDumpDebugInfoArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Dump operation. */
    VisorDumpDebugInfoOperation operation;

    /** */
    private List<Long> pageIds;

    /** */
    private String pathToDump;

    /** List of dump debug action. */
    private Collection<DumpAction> dumpActions;

    /**
     * Default constructor.
     */
    public VisorDumpDebugInfoArg() {
        // No-op.
    }

    /**
     * @param operation Dump operation.
     * @param pageIds List of page id for find their history.
     * @param pathToDump Custom path to store dump.
     * @param dumpActions Action of dump debug info.
     */
    public VisorDumpDebugInfoArg(
        VisorDumpDebugInfoOperation operation,
        List<Long> pageIds,
        String pathToDump,
        Collection<DumpAction> dumpActions
    ) {
        this.operation = operation;
        this.pageIds = pageIds == null ? Collections.emptyList() : pageIds;
        this.pathToDump = pathToDump;
        this.dumpActions = dumpActions;
    }

    /**
     * @return Dump operation.
     */
    public VisorDumpDebugInfoOperation getOperation() {
        return operation;
    }

    /**
     * @return List of page id for searching their history.
     */
    public List<Long> getPageIds() {
        return pageIds;
    }

    /**
     * @return Custom path to store debug info.
     */
    public String getPathToDump() {
        return pathToDump;
    }

    /**
     * @return Dump actions.
     */
    public Collection<DumpAction> getDumpActions() {
        return dumpActions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, operation);
        U.writeCollection(out, pageIds);
        U.writeEnumCollection(out, dumpActions);
        out.writeObject(pathToDump);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        operation = U.readEnum(in, VisorDumpDebugInfoOperation.class);
        pageIds = U.readList(in);
        dumpActions = U.readEnumList(in, DumpAction.class);
        pathToDump = (String)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDumpDebugInfoArg.class, this);
    }

    /**
     * List of possible dump action.
     */
    public enum DumpAction {
        /** Print debug info to log. */
        PRINT_TO_LOG,
        /** Print debug info to file. */
        PRINT_TO_FILE
    }
}
