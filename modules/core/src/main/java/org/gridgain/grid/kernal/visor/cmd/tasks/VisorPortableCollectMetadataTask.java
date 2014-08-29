/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task that collects portables metadata.
 */
@GridInternal
public class VisorPortableCollectMetadataTask extends VisorOneNodeTask<Long, GridBiTuple<Long, Collection<VisorPortableMetadata>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorPortableCollectMetadataJob job(Long lastUpdate) {
        return new VisorPortableCollectMetadataJob(lastUpdate);
    }

    /** Job that collect portables metadata on node. */
    private static class VisorPortableCollectMetadataJob extends VisorJob<Long, GridBiTuple<Long, Collection<VisorPortableMetadata>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create job with given argument. */
        private VisorPortableCollectMetadataJob(Long lastUpdate) {
            super(lastUpdate);
        }

        /** {@inheritDoc} */
        @Override protected GridBiTuple<Long, Collection<VisorPortableMetadata>> run(Long lastUpdate) throws GridException {
            final List<VisorPortableMetadata> data = new ArrayList<>();
            
            final GridBiTuple<Long, Collection<VisorPortableMetadata>> res = new GridBiTuple<>(0L, (Collection<VisorPortableMetadata>) data);

            GridPortables p = g.portables();

            for(GridPortableMetadata metadata: p.metadata()) {
                VisorPortableMetadata type = new VisorPortableMetadata();

                type.typeName(metadata.typeName());

                type.typeID(p.typeId(metadata.typeName()));

                final List<VisorPortableMetadataField> fields = new ArrayList<>();

                for (String fieldName: metadata.fields()) {
                    VisorPortableMetadataField field = new VisorPortableMetadataField();

                    field.name(fieldName);
                    field.fieldType(metadata.fieldTypeName(fieldName));

                    fields.add(field);
                }

                type.fields(fields);

                data.add(type);
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorPortableCollectMetadataJob.class, this);
        }
    }
}
