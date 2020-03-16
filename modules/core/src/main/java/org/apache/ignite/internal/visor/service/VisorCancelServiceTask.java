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

import java.util.Optional;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.services.ServiceDescriptor;

/**
 * Task for cancel services with specified name.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCancelServiceTask extends VisorOneNodeTask<VisorCancelServiceTaskArg, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCancelServiceJob job(VisorCancelServiceTaskArg arg) {
        return new VisorCancelServiceJob(arg, debug);
    }

    /**
     * Job for cancel services with specified name.
     */
    private static class VisorCancelServiceJob extends VisorJob<VisorCancelServiceTaskArg, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        protected VisorCancelServiceJob(VisorCancelServiceTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Boolean run(final VisorCancelServiceTaskArg arg) {
            IgniteServices services = ignite.services();

            String svcName = arg.getName();

            Optional<ServiceDescriptor> svc = services.serviceDescriptors().stream()
                .filter(d -> d.name().equalsIgnoreCase(svcName))
                .findFirst();

            if (!svc.isPresent())
                return false;

            try {
                services.cancel(svcName);
            }
            catch (IgniteException e) {
                IgniteLogger log = ignite.log().getLogger(VisorCancelServiceTask.class);

                log.warning("Error on service cance.", e);

                return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCancelServiceJob.class, this);
        }
    }
}
