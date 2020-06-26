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

package org.apache.ignite.internal.commandline.meta.tasks;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.commandline.cache.CheckIndexInlineSizes;
import org.apache.ignite.internal.commandline.meta.subcommands.MetadataUpdateCommand;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

/**
 * Task for {@link MetadataUpdateCommand} command.
 */
@GridInternal
public class MetadataUpdateTask extends VisorMultiNodeTask<MetadataMarshalled, MetadataMarshalled, MetadataMarshalled> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<MetadataMarshalled, MetadataMarshalled> job(MetadataMarshalled arg) {
        return new MetadataUpdateJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected MetadataMarshalled reduce0(List<ComputeJobResult> results) {
        if (results.size() != 1)
            throw new IgniteException("Invalid job results. Expected exactly 1 result, but was: " + results);

        if (results.get(0).getException() != null)
            throw results.get(0).getException();
        else
            return results.get(0).getData();
    }

    /**
     * Job for {@link CheckIndexInlineSizes} command.
     */
    private static class MetadataUpdateJob extends VisorJob<MetadataMarshalled, MetadataMarshalled> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected MetadataUpdateJob(@Nullable MetadataMarshalled arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected MetadataMarshalled run(@Nullable MetadataMarshalled arg) throws IgniteException {
            ignite.context().security().authorize(null, SecurityPermission.ADMIN_METADATA_OPS);

            assert Objects.nonNull(arg);

            byte[] marshalled = arg.metadataMarshalled();

            try {
                BinaryMetadata meta = U.unmarshal(
                    ignite.context(),
                    marshalled,
                    U.resolveClassLoader(ignite.context().config()));

                ((CacheObjectBinaryProcessorImpl)ignite.context().cacheObjects()).binaryContext()
                    .updateMetadata(meta.typeId(), meta, false);

                return new MetadataMarshalled(null, meta);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
