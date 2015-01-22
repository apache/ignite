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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.managed.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Service deployment.
 */
public class GridServiceDeployment implements GridCacheInternal, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID nodeId;

    /** Service configuration. */
    private ManagedServiceConfiguration cfg;

    /**
     * @param nodeId Node ID.
     * @param cfg Service configuration.
     */
    public GridServiceDeployment(UUID nodeId, ManagedServiceConfiguration cfg) {
        this.nodeId = nodeId;
        this.cfg = cfg;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Service configuration.
     */
    public ManagedServiceConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridServiceDeployment that = (GridServiceDeployment)o;

        if (cfg != null ? !cfg.equals(that.cfg) : that.cfg != null)
            return false;

        if (nodeId != null ? !nodeId.equals(that.nodeId) : that.nodeId != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId != null ? nodeId.hashCode() : 0;

        res = 31 * res + (cfg != null ? cfg.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceDeployment.class, this);
    }
}
