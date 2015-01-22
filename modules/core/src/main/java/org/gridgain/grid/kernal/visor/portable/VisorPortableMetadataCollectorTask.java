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

package org.gridgain.grid.kernal.visor.portable;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.portables.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Task that collects portables metadata.
 */
@GridInternal
public class VisorPortableMetadataCollectorTask extends VisorOneNodeTask<Long, IgniteBiTuple<Long, Collection<VisorPortableMetadata>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorPortableCollectMetadataJob job(Long lastUpdate) {
        return new VisorPortableCollectMetadataJob(lastUpdate, debug);
    }

    /** Job that collect portables metadata on node. */
    private static class VisorPortableCollectMetadataJob extends VisorJob<Long, IgniteBiTuple<Long, Collection<VisorPortableMetadata>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param lastUpdate Time data was collected last time.
         * @param debug Debug flag.
         */
        private VisorPortableCollectMetadataJob(Long lastUpdate, boolean debug) {
            super(lastUpdate, debug);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Long, Collection<VisorPortableMetadata>> run(Long lastUpdate) throws IgniteCheckedException {
            final IgnitePortables p = g.portables();

            final Collection<VisorPortableMetadata> data = new ArrayList<>(p.metadata().size());

            for(PortableMetadata metadata: p.metadata()) {
                final VisorPortableMetadata type = new VisorPortableMetadata();

                type.typeName(metadata.typeName());

                type.typeId(p.typeId(metadata.typeName()));

                final Collection<VisorPortableMetadataField> fields = new ArrayList<>(metadata.fields().size());

                for (String fieldName: metadata.fields()) {
                    final VisorPortableMetadataField field = new VisorPortableMetadataField();

                    field.fieldName(fieldName);
                    field.fieldTypeName(metadata.fieldTypeName(fieldName));

                    fields.add(field);
                }

                type.fields(fields);

                data.add(type);
            }

            return new IgniteBiTuple<>(0L, data);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorPortableCollectMetadataJob.class, this);
        }
    }
}
