/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.gui.dto.*;
import org.gridgain.grid.mongo.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task for reset mongo.
 */
@GridInternal
public class VisorMongoRangesTask extends VisorOneNodeTask<String, Map<String, List<VisorMongoRange>>> {
    /**
     * Job that reset mongo
     */
    private static class VisorMongoRangesJob extends VisorJob<String, Map<String, List<VisorMongoRange>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorMongoRangesJob(String arg) {
            super(arg);
        }

        @Override protected  Map<String, List<VisorMongoRange>> run(String arg) throws GridException {
            Map<String, List<VisorMongoRange>> total = new HashMap<>();

            GridMongo mongo = null; // TODO: gg-mongo g.mongo()

            if (mongo == null)
                throw new GridException("Failed to collect Mongo ranges: Mongo not found");

            Set<String> namespaces = new HashSet<String>();

            for (GridMongoDatabaseMetrics dbm: mongo.metrics().databaseMetrics())
                for (GridMongoCollectionMetrics cm: dbm.collectionMetrics())
                    namespaces.add(dbm.name() + '.' + cm.name());

            for (String namespace: namespaces) {
                Collection<GridMongoRange> ranges = mongo.ranges(namespace);

                ArrayList<VisorMongoRange> resRanges = new ArrayList<>();

                for (GridMongoRange r: ranges) {
                    ArrayList<UUID> nids = new ArrayList<>();

                    for (GridNode n: mongo.mapRangeToNodes(r))
                        nids.add(n.id());

                    resRanges.add(new VisorMongoRange(r.database(), r.collection(), r.id(), r.size(), nids));
                }

                total.put(namespace, resRanges);
            }

            return total;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorMongoRangesJob.class, this);
        }
    }

    @Override protected VisorMongoRangesJob job(String arg) {
        return new VisorMongoRangesJob(arg);
    }
}
