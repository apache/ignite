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
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.NotNull;

/**
 * Service single node deployment result.
 * <p/>
 * Contains count of deployed service instances on single node and deployment errors if exist.
 */
public class ServiceSingleNodeDeploymentResult implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Count of service's instances. */
    @Order(0)
    int cnt;

    /** Serialized exceptions. */
    @Order(1)
    Collection<byte[]> errors;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServiceSingleNodeDeploymentResult() {
    }

    /**
     * @param cnt Count of service's instances.
     */
    public ServiceSingleNodeDeploymentResult(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Count of service's instances.
     */
    public int count() {
        return cnt;
    }

    /**
     * @param cnt Count of service's instances.
     */
    public void count(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Serialized exceptions.
     */
    @NotNull public Collection<byte[]> errors() {
        return errors != null ? errors : Collections.emptyList();
    }

    /**
     * @param errors Serialized exceptions.
     */
    public void errors(Collection<byte[]> errors) {
        this.errors = errors;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 169;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceSingleNodeDeploymentResult.class, this);
    }
}
