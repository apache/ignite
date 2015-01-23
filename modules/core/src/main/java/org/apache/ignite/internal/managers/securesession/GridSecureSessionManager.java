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

package org.apache.ignite.internal.managers.securesession;

import org.apache.ignite.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.managers.security.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * This interface defines a grid secure session manager.
 */
public interface GridSecureSessionManager extends GridManager {
    /**
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @param tok Token.
     * @param params Parameters.
     * @return Validated secure session or {@code null} if session is not valid.
     * @throws IgniteCheckedException If error occurred.
     */
    public GridSecureSession validateSession(GridSecuritySubjectType subjType, UUID subjId, @Nullable byte[] tok,
        @Nullable Object params) throws IgniteCheckedException;

    /**
     * Generates secure session token.
     *
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @param subjCtx Authentication subject context.
     * @param params Params.
     * @return Generated session token.
     */
    public byte[] updateSession(GridSecuritySubjectType subjType, UUID subjId, GridSecurityContext subjCtx,
        @Nullable Object params) throws IgniteCheckedException;
}
