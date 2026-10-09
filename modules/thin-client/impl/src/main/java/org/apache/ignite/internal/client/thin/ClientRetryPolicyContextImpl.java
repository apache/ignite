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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientOperationType;
import org.apache.ignite.client.ClientRetryPolicyContext;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 * Retry policy context.
 */
class ClientRetryPolicyContextImpl implements ClientRetryPolicyContext {
    /** */
    private final ClientConfiguration configuration;

    /** */
    private final ClientOperationType operation;

    /** */
    private final int iteration;

    /** */
    private final ClientConnectionException exception;

    /**
     * Constructor.
     *
     * @param configuration Configuration.
     * @param operation Operation.
     * @param iteration Iteration.
     * @param exception Exception.
     */
    public ClientRetryPolicyContextImpl(ClientConfiguration configuration, ClientOperationType operation, int iteration,
            ClientConnectionException exception) {
        this.configuration = configuration;
        this.operation = operation;
        this.iteration = iteration;
        this.exception = exception;
    }

    /** {@inheritDoc} */
    @Override public ClientConfiguration configuration() {
        return configuration;
    }

    /** {@inheritDoc} */
    @Override public ClientOperationType operation() {
        return operation;
    }

    /** {@inheritDoc} */
    @Override public int iteration() {
        return iteration;
    }

    /** {@inheritDoc} */
    @Override public ClientConnectionException exception() {
        return exception;
    }
}
