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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import java.util.function.Supplier;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteRunnable;

/** Maintains the set of active cluster {@link IgniteProductFeatures} used by Rolling Upgrade logic. */
public class IgniteFeatureManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteProductFeatures locVerFeatures;

    /** */
    private final GridFutureAdapter<Void> locVerFeaturesActivationFut;

    /** */
    private volatile IgniteProductFeatures activeFeatures;

    /** */
    public IgniteFeatureManager(GridKernalContext ctx, Supplier<IgniteProductFeatures> locVerFeaturesProv) {
        this.ctx = ctx;
        locVerFeatures = locVerFeaturesProv.get();
        locVerFeaturesActivationFut = new GridFutureAdapter<>();
    }

    /** @return The set of features declared by the local node's product version. */
    public IgniteProductFeatures localVersionFeatures() {
        return locVerFeatures;
    }

    /** @return The set of features currently active in the cluster. */
    public IgniteProductFeatures activeFeatures() {
        final IgniteProductFeatures finalActiveFeatures = activeFeatures;

        if (finalActiveFeatures == null) {
            throw new IllegalStateException("Local node features are not yet initialized [locNodeId=" +
                ctx.discovery().localNode().id() + ']');
        }

        return finalActiveFeatures;
    }

    /** @return {@code true} if the specified {@link IgniteFeature} is active in the cluster; {@code false} otherwise. */
    public boolean isActive(IgniteFeature feature) {
        final IgniteProductFeatures finalActiveFeatures = activeFeatures;

        if (finalActiveFeatures == null) {
            throw new IllegalStateException("Local node features are not yet initialized [locNodeId=" +
                ctx.discovery().localNode().id() + ']');
        }

        return finalActiveFeatures.contains(feature);
    }

    /** Registers the specified listener to be notified when the {@link IgniteFeature} is activated. */
    public void listenActivation(IgniteFeature feature, IgniteRunnable lsnr) {
        assert locVerFeatures.contains(feature);

        if (activeFeatures.contains(feature))
            lsnr.run();
        else
            locVerFeaturesActivationFut.listen(lsnr);
    }

    /** */
    public void onGridDataReceived(IgniteProductFeatures activeClusterFeatures) {
        if (locVerFeatures.equals(activeClusterFeatures))
            activateLocalVersionFeatures();
        else
            this.activeFeatures = activeClusterFeatures;
    }

    /** */
    public void onLocalJoin() {
        if (activeFeatures == null)
            activateLocalVersionFeatures();
    }

    /** */
    public synchronized void activateLocalVersionFeatures() {
        if (locVerFeaturesActivationFut.isDone())
            return;

        activeFeatures = locVerFeatures;

        locVerFeaturesActivationFut.onDone();
    }
}
