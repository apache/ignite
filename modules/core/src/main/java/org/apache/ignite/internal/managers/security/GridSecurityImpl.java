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

package org.apache.ignite.internal.managers.security;

import org.apache.ignite.*;
import org.apache.ignite.plugin.security.*;
import org.gridgain.grid.kernal.*;

import java.io.*;
import java.util.*;

/**
 * Implementation of grid security interface.
 */
public class GridSecurityImpl implements GridSecurity, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Security manager. */
    private GridSecurityManager secMgr;

    /** Context. */
    private GridKernalContext ctx;

    /**
     * Required by {@link Externalizable}.
     */
    public GridSecurityImpl() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    public GridSecurityImpl(GridKernalContext ctx) {
        this.secMgr = ctx.security();
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridSecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return secMgr.authenticatedSubjects();
    }

    /** {@inheritDoc} */
    @Override public GridSecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return secMgr.authenticatedSubject(subjId);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridKernalContext)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        return ctx.grid().security();
    }
}
