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

package org.apache.ignite.internal.processors.platform.dotnet;

import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.services.PlatformAbstractService;

/**
 * Interop .Net service.
 */
public class PlatformDotNetServiceImpl extends PlatformAbstractService implements PlatformDotNetService {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor for serialization.
     */
    public PlatformDotNetServiceImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param svc Service.
     * @param ctx Context.
     * @param srvKeepBinary Whether to keep objects binary on server if possible.
     */
    public PlatformDotNetServiceImpl(Object svc, PlatformContext ctx, boolean srvKeepBinary) {
        super(svc, ctx, srvKeepBinary, null);
    }

    /**
     * Constructor.
     *
     * @param svc Service.
     * @param ctx Context.
     * @param srvKeepBinary Whether to keep objects binary on server if possible.
     * @param interceptors Service call interceptors.
     */
    public PlatformDotNetServiceImpl(Object svc, PlatformContext ctx, boolean srvKeepBinary, Object interceptors) {
        super(svc, ctx, srvKeepBinary, interceptors);
    }

    /**
     * @return Service itself
     */
    public Object getInternalService() {
        return svc;
    }

    /**
     * @return Service call interceptors.
     */
    public Object getInterceptors() {
        return interceptors;
    }
}
