/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.property.tasks;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_READ_DISTRIBUTED_PROPERTY;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/**
 * Task for property operations.
 */
@GridInternal
public class PropertiesListTask extends VisorMultiNodeTask<NoArg, PropertiesListResult, PropertiesListResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, PropertiesListResult> job(NoArg arg) {
        return new PropertiesListJob(debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected PropertiesListResult reduce0(List<ComputeJobResult> results) {
        if (results.isEmpty())
            throw new IgniteException("Empty job results");

        if (results.size() > 1)
            throw new IgniteException("Invalid job results: " + results);

        if (results.get(0).getException() != null)
            throw results.get(0).getException();
        else
            return results.get(0).getData();
    }

    /**
     * Job for property operations (get/set).
     */
    private static class PropertiesListJob extends VisorJob<NoArg, PropertiesListResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param debug Debug.
         */
        protected PropertiesListJob(boolean debug) {
            super(null, debug);
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            return systemPermissions(ADMIN_READ_DISTRIBUTED_PROPERTY);
        }

        /** {@inheritDoc} */
        @Override protected PropertiesListResult run(@Nullable NoArg arg) {
            return new PropertiesListResult(
                ignite.context().distributedConfiguration().properties().stream()
                    .map(pd -> pd.getName())
                    .collect(Collectors.toList())
            );
        }
    }
}
