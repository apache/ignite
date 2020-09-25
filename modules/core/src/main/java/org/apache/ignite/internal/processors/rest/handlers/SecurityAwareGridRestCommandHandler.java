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

package org.apache.ignite.internal.processors.rest.handlers;

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;

/**
 * Handler setups a security context related to a subject id from a request.
 */
public class SecurityAwareGridRestCommandHandler implements GridRestCommandHandler {
    /** */
    private final GridKernalContext ctx;
    /** */
    private final GridRestCommandHandler original;

    /**
     * @param ctx GridKernalContext.
     * @param original the original handler.
     */
    public SecurityAwareGridRestCommandHandler(GridKernalContext ctx, GridRestCommandHandler original) {
        this.ctx = ctx;
        this.original = original;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return original.supportedCommands();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        try(OperationSecurityContext c = ctx.security().withContext(req.clientId())){
            return original.handleAsync(req);
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return original.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        Object hnd = obj;

        if(obj instanceof SecurityAwareGridRestCommandHandler)
            hnd = ((SecurityAwareGridRestCommandHandler)obj).original;

        return original.equals(hnd);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return original.toString();
    }
}
