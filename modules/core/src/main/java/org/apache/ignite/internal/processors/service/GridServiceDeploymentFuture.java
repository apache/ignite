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

import java.io.Serializable;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Service deployment future.
 */
public class GridServiceDeploymentFuture<T extends Serializable> extends GridFutureAdapter<Object> {
    /** */
    private final ServiceConfiguration cfg;

    /** */
    private final T srvcId;

    /**
     * @param cfg Configuration.
     * @param srvcId Service id.
     */
    public GridServiceDeploymentFuture(ServiceConfiguration cfg, T srvcId) {
        this.cfg = cfg;
        this.srvcId = srvcId;
    }

    /**
     * @return Service configuration.
     */
    ServiceConfiguration configuration() {
        return cfg;
    }

    /**
     * @return Service id.
     */
    public T serviceId() {
        return srvcId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceDeploymentFuture.class, this);
    }
}
