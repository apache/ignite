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

package org.apache.ignite.internal.management.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ContentionTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cluster infos. */
    @Order(0)
    List<ContentionJobResult> clusterInfos;

    /** Exceptions. */
    @Order(1)
    Map<UUID, Exception> exceptions;

    /**
     * @param clusterInfos Cluster infos.
     * @param exceptions Exceptions.
     */
    public ContentionTaskResult(List<ContentionJobResult> clusterInfos,
        Map<UUID, Exception> exceptions) {
        this.clusterInfos = clusterInfos;
        this.exceptions = exceptions;
    }

    /**
     * For externalization only.
     */
    public ContentionTaskResult() {
    }

    /**
     * @return Cluster infos.
     */
    public Collection<ContentionJobResult> jobResults() {
        return clusterInfos;
    }

    /**
     * @return Exceptions.
     */
    public Map<UUID, Exception> exceptions() {
        return exceptions;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContentionTaskResult.class, this);
    }
}
