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

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.verify.ContentionInfo;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 *
 */
public class VisorContentionTaskResult extends VisorDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cluster infos. */
    private List<VisorContentionJobResult> clusterInfos;

    /** Exceptions. */
    private Map<UUID, Exception> exceptions;

    /**
     * @param clusterInfos Cluster infos.
     * @param exceptions Exceptions.
     */
    public VisorContentionTaskResult(List<VisorContentionJobResult> clusterInfos,
        Map<UUID, Exception> exceptions) {
        this.clusterInfos = clusterInfos;
        this.exceptions = exceptions;
    }

    /**
     * For externalization only.
     */
    public VisorContentionTaskResult() {
    }

    /**
     * @return Cluster infos.
     */
    public Collection<VisorContentionJobResult> jobResults() {
        return clusterInfos;
    }

    /**
     * @return Collection of {@link ContentionInfo} collected during task execution.
     */
    public Collection<ContentionInfo> getInfos() {
        return clusterInfos.stream().map(VisorContentionJobResult::info).collect(Collectors.toList());
    }

    /**
     * @return Exceptions.
     */
    public Map<UUID, Exception> exceptions() {
        return exceptions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, clusterInfos);
        U.writeMap(out, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in
    ) throws IOException, ClassNotFoundException {
        clusterInfos = U.readList(in);
        exceptions = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorContentionTaskResult.class, this);
    }
}
