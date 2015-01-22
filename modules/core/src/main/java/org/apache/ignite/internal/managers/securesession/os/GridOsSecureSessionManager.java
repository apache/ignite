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

package org.apache.ignite.internal.managers.securesession.os;

import org.apache.ignite.*;
import org.apache.ignite.plugin.security.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.managers.securesession.*;
import org.apache.ignite.internal.managers.security.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op implementation for {@link GridSecureSessionManager}.
 */
public class GridOsSecureSessionManager extends GridNoopManagerAdapter implements GridSecureSessionManager {
    /** Empty bytes. */
    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * @param ctx Kernal context.
     */
    public GridOsSecureSessionManager(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public GridSecureSession validateSession(GridSecuritySubjectType subjType, UUID subjId,
        @Nullable byte[] tok,
        @Nullable Object params) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public byte[] updateSession(GridSecuritySubjectType subjType, UUID subjId, GridSecurityContext subjCtx,
        @Nullable Object params) {
        return EMPTY_BYTES;
    }
}
