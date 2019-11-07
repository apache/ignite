/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.splitter;

import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentLocation;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationRegistry;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class Fragment {
    public final RelNode rel;

    public FragmentLocation fragmentLocation;

    public Fragment(RelNode rel) {
        this.rel = rel;
    }

    public void init(Context ctx) {
        RelMetadataQueryEx mq = RelMetadataQueryEx.instance();

        fragmentLocation = mq.getFragmentLocation(rel);

        AffinityTopologyVersion topVer = topologyVersion(ctx);

        if (fragmentLocation.location == null) {
            if (!isRoot())
                fragmentLocation.location = registry(ctx).random(topVer);
            else if (!F.isEmpty(fragmentLocation.remoteInputs))
                fragmentLocation.location = registry(ctx).single(topVer);
        }
        else {
            try {
                fragmentLocation.location = fragmentLocation.location.deduplicate();
            }
            catch (LocationMappingException e) {
                throw new IgniteSQLException("Failed to map fragment to location, partition lost.", e);
            }
        }

        if (!F.isEmpty(fragmentLocation.remoteInputs)) {
            for (Receiver input : fragmentLocation.remoteInputs)
                input.init(fragmentLocation, mq);
        }
    }

    private boolean isRoot() {
        return !(rel instanceof Sender);
    }

    private LocationRegistry registry(Context ctx) {
        return ctx.unwrap(LocationRegistry.class);
    }

    private AffinityTopologyVersion topologyVersion(Context ctx) {
        return ctx.unwrap(AffinityTopologyVersion.class);
    }

    public void reset() {
        if (rel instanceof Sender)
            ((Sender) rel).reset();

        if (fragmentLocation != null && !F.isEmpty(fragmentLocation.remoteInputs)) {
            for (Receiver receiver : fragmentLocation.remoteInputs) {
                receiver.reset();
            }
        }

        fragmentLocation = null;
    }
}
