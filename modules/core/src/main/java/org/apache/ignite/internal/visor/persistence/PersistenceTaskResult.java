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

package org.apache.ignite.internal.visor.persistence;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/** */
public class PersistenceTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean inMaintenanceMode;

    /** */
    private boolean maintenanceTaskCompleted;

    /** */
    private List<String> handledCaches;

    /** */
    private List<String> failedToHandleCaches;

    /** */
    private Map<String, IgniteBiTuple<Boolean, Boolean>> cachesInfo;

    /** */
    public PersistenceTaskResult() {
        // No-op.
    }

    /**
     *
     */
    public PersistenceTaskResult(boolean inMaintenanceMode) {
        this.inMaintenanceMode = inMaintenanceMode;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(inMaintenanceMode);
        out.writeBoolean(maintenanceTaskCompleted);
        U.writeCollection(out, handledCaches);
        U.writeCollection(out, failedToHandleCaches);
        U.writeMap(out, cachesInfo);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        inMaintenanceMode = in.readBoolean();
        maintenanceTaskCompleted = in.readBoolean();
        handledCaches = U.readList(in);
        failedToHandleCaches = U.readList(in);
        cachesInfo = U.readMap(in);
    }

    /** */
    public boolean inMaintenanceMode() {
        return inMaintenanceMode;
    }

    /** */
    public boolean maintenanceTaskCompleted() {
        return maintenanceTaskCompleted;
    }

    /** */
    public void maintenanceTaskCompleted(boolean maintenanceTaskCompleted) {
        this.maintenanceTaskCompleted = maintenanceTaskCompleted;
    }

    /** */
    public List<String> handledCaches() {
        return handledCaches;
    }

    /** */
    public void handledCaches(List<String> handledCaches) {
        this.handledCaches = handledCaches;
    }

    /** */
    public List<String> failedCaches() {
        return failedToHandleCaches;
    }

    /** */
    public void failedCaches(List<String> failedToHandleCaches) {
        this.failedToHandleCaches = failedToHandleCaches;
    }

    /** */
    public Map<String, IgniteBiTuple<Boolean, Boolean>> cachesInfo() {
        return cachesInfo;
    }

    /** */
    public void cachesInfo(Map<String, IgniteBiTuple<Boolean, Boolean>> cachesInfo) {
        this.cachesInfo = cachesInfo;
    }
}
