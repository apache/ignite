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

package org.apache.ignite.internal.visor.service;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.services.ServiceDescriptor;

/**
 * Task for collect topology service configuration.
 */
@GridInternal
public class VisorServiceTask extends VisorOneNodeTask<Void, Collection<VisorServiceDescriptor>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorServiceJob job(Void arg) {
        return new VisorServiceJob(arg, debug);
    }

    /**
     * Job for collect topology service configuration.
     */
    private static class VisorServiceJob extends VisorJob<Void, Collection<VisorServiceDescriptor>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        protected VisorServiceJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorServiceDescriptor> run(final Void arg) {
            Collection<ServiceDescriptor> services = ignite.services().serviceDescriptors();

            Collection<VisorServiceDescriptor> res = new ArrayList<>(services.size());

            for (ServiceDescriptor srvc: services)
                res.add(new VisorServiceDescriptor(srvc));

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorServiceJob.class, this);
        }
    }
}
