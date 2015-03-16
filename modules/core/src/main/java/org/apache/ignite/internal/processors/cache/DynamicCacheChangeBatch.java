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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Cache change batch.
 */
public class DynamicCacheChangeBatch implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Change requests. */
    @GridToStringInclude
    private Collection<DynamicCacheChangeRequest> reqs;

    /** Client nodes map. Used in discovery data exchange. */
    @GridToStringInclude
    private Map<String, Map<UUID, Boolean>> clientNodes;

    /**
     * @param reqs Requests.
     */
    public DynamicCacheChangeBatch(
        Collection<DynamicCacheChangeRequest> reqs
    ) {
        this.reqs = reqs;
    }

    /**
     * @return Collection of change requests.
     */
    public Collection<DynamicCacheChangeRequest> requests() {
        return reqs;
    }

    /**
     * @return Client nodes map.
     */
    public Map<String, Map<UUID, Boolean>> clientNodes() {
        return clientNodes;
    }

    /**
     * @param clientNodes Client nodes map.
     */
    public void clientNodes(Map<String, Map<UUID, Boolean>> clientNodes) {
        this.clientNodes = clientNodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeBatch.class, this);
    }
}
