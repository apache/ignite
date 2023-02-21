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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.commandline.property.PropertyArgs;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.commandline.property.PropertyArgs.Action.GET;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_READ_DISTRIBUTED_PROPERTY;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_WRITE_DISTRIBUTED_PROPERTY;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/**
 * Task for property operations.
 */
@GridInternal
public class PropertyTask extends VisorMultiNodeTask<PropertyArgs, PropertyOperationResult, PropertyOperationResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<PropertyArgs, PropertyOperationResult> job(PropertyArgs arg) {
        return new PropertyJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected PropertyOperationResult reduce0(List<ComputeJobResult> results) {
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
     * Job for getting binary metadata.
     */
    private static class PropertyJob extends VisorJob<PropertyArgs, PropertyOperationResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected PropertyJob(@Nullable PropertyArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            return systemPermissions(((PropertyArgs)argument(0)).action() == GET
                ? ADMIN_READ_DISTRIBUTED_PROPERTY
                : ADMIN_WRITE_DISTRIBUTED_PROPERTY
            );
        }

        /** {@inheritDoc} */
        @Override protected PropertyOperationResult run(@Nullable PropertyArgs arg) {
            DistributedChangeableProperty<Serializable> prop =
                ignite.context().distributedConfiguration().property(arg.name());

            if (prop == null)
                throw new IllegalArgumentException("Property doesn't not exist [name=" + arg.name() + ']');

            switch (arg.action()) {
                case GET:
                    return new PropertyOperationResult(Objects.toString(prop.get()));

                case SET:
                    try {
                        prop.propagate(prop.parse(arg.value()));
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    return new PropertyOperationResult(null);

                default:
                    assert false : "unexpected state";

                    return null;
            }
        }
    }
}
