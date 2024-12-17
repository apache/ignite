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

package org.apache.ignite.internal.processors.query.calcite.exec;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.session.SessionContextProvider;

/** Service that helps inject Ignite resources to Calcite functions. */
public class InjectResourcesService extends AbstractService {
    /** */
    private final GridResourceProcessor rsrcProc;

    /** */
    public InjectResourcesService(GridKernalContext ctx) {
        super(ctx);

        rsrcProc = ctx.resource();
    }

    /**
     * Inject resources to object contained a user defined function.

     * @param obj Object to inject resources.
     * @param sesCtxProv Session context provider.
     * @throws IgniteCheckedException If failed to inject.
     */
    void injectToUdf(Object obj, SessionContextProvider sesCtxProv) throws IgniteCheckedException {
        rsrcProc.injectToUdf(obj, sesCtxProv);
    }
}
