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

package org.apache.ignite.internal.management.meta;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;

/**
 * Task for MetadataListCommand and MetadataDetailsCommand commands.
 */
@GridInternal
public class MetadataInfoTask extends VisorMultiNodeTask<IgniteDataTransferObject, MetadataListResult, MetadataListResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<IgniteDataTransferObject, MetadataListResult> job(IgniteDataTransferObject arg) {
        return new MetadataListJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected MetadataListResult reduce0(List<ComputeJobResult> results) {
        if (results.isEmpty())
            throw new IgniteException("Empty job results");

        if (results.size() > 1)
            throw new IgniteException("Invalid job results: " + results);

        if (results.get(0).getException() != null)
            throw results.get(0).getException();
        else
            return results.get(0).getData();
    }

    /** */
    private static class MetadataListJob extends VisorJob<IgniteDataTransferObject, MetadataListResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected MetadataListJob(IgniteDataTransferObject arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected MetadataListResult run(IgniteDataTransferObject arg0) throws IgniteException {
            if (arg0 instanceof NoArg) {
                // returns full metadata
                return new MetadataListResult(
                    ((CacheObjectBinaryProcessorImpl)ignite.context().cacheObjects()).binaryMetadata());
            }
            else {
                MetaDetailsCommandArg arg = (MetaDetailsCommandArg)arg0;

                // returns specified metadata
                int typeId = typeId(ignite.context(), arg.typeId(), arg.typeName());

                BinaryMetadata binMeta = ((CacheObjectBinaryProcessorImpl)ignite.context().cacheObjects()).binaryMetadata(typeId);

                if (binMeta == null)
                    throw new IgniteException("Failed to get metadata, type not found: " + typeId);

                return new MetadataListResult(Collections.singleton(binMeta));
            }
        }
    }

    /** */
    public static int typeId(GridKernalContext ctx, int typeId, String typeName) {
        if (typeId != 0)
            return typeId;
        else
            return ctx.cacheObjects().typeId(typeName);
    }
}
