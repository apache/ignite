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

package org.apache.ignite.internal.processors.securesession;

import org.apache.ignite.internal.processors.security.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Secure session object.
 */
public class GridSecureSession {
    /** Authentication subject context returned by authentication SPI. */
    private GridSecurityContext authSubjCtx;

    /** Session creation time. */
    private byte[] sesTok;

    /**
     * @param authSubjCtx Authentication subject context.
     * @param sesTok Session token.
     */
    public GridSecureSession(GridSecurityContext authSubjCtx, byte[] sesTok) {
        this.authSubjCtx = authSubjCtx;
        this.sesTok = sesTok;
    }

    /**
     * @return Authentication subject context returned by authentication SPI.
     */
    public GridSecurityContext authenticationSubjectContext() {
        return authSubjCtx;
    }

    /**
     * @return Session creation time.
     */
    public byte[] sessionToken() {
        return sesTok;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSecureSession.class, this);
    }
}
