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
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;

/**
 * Argument for {@link VisorBaselineTask}.
 */
public class VisorDumpPageHistoryTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    VisorDumpOperation operation;

    /** */
    private List<Long> pageIds;

    /** */
    private String fileToDump;

    private List<DumpAction> dumpActions;

    /**
     * Default constructor.
     */
    public VisorDumpPageHistoryTaskArg() {
        // No-op.
    }

    /**
     * @param topVer Topology version.
     * @param pageIds Consistent ids.
     * @param autoAdjustSettings Baseline autoadjustment settings.
     */
    public VisorDumpPageHistoryTaskArg(
        VisorDumpOperation operation,
        List<Long> pageIds,
        String fileToDump,
        List<DumpAction> dumpActions
    ) {
        this.operation = operation;
        this.pageIds = pageIds;
        this.fileToDump = fileToDump;
        this.dumpActions = dumpActions;
    }

    public List<Long> getPageIds() {
        return pageIds;
    }

    public String getFileToDump() {
        return fileToDump;
    }

    public List<DumpAction> getDumpActions() {
        return dumpActions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, pageIds);
        U.writeEnumCollection(out, dumpActions);
        out.writeObject(fileToDump);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        pageIds = U.readList(in);
        dumpActions = U.readEnumList(in, DumpAction.class);
        fileToDump = (String)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDumpPageHistoryTaskArg.class, this);
    }

    public enum DumpAction {
        PRINT_TO_LOG,
        PRINT_TO_FILE
    }
}
