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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeClusterData;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

/** Maintains the set of active cluster {@link IgniteComponentFeatureSet} used by Rolling Upgrade logic. */
public class IgniteFeatureManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteNodeFeatureSet locVerFeatures;

    /**
     * During the RU process, updated nodes operate in accordance with both the old logical version (prior to RU completion)
     * and the new one (after RU completion). This variable stores the features of the previous version under which this
     * node operated.
     *
     * <p>
     *     If a node joins the cluster after the Rolling Upgrade has been finalized, this value is inherited from the
     *     existing cluster nodes (after Rolling Upgrade finalization, all cluster nodes are guaranteed to hold the same
     *     value for this field.
     *</p>
     */
    @Nullable private volatile IgniteNodeFeatureSet prevActiveFeatures;

    /** */
    private volatile IgniteNodeFeatureSet activeFeatures;

    /** */
    private final GridFutureAdapter<Void> locVerFeaturesActivationFut;

    /** */
    public IgniteFeatureManager(GridKernalContext ctx, IgniteCoreFeatureSet coreFeatures) {
        this.ctx = ctx;
        this.locVerFeatures = collectLocalVersionFeatures(ctx, coreFeatures);
        locVerFeaturesActivationFut = new GridFutureAdapter<>();
    }

    /** @return The set of features declared by the local node's product version. */
    public IgniteNodeFeatureSet localVersionFeatures() {
        return locVerFeatures;
    }

    /** @return Active functions of the specified component. */
    @Nullable public IgniteComponentFeatureSet activeComponentFeatures(String cmpName) {
        return activeFeatures().componentFeatures(cmpName);
    }

    /** @return The set of features currently active in the cluster. */
    public IgniteNodeFeatureSet activeFeatures() {
        final IgniteNodeFeatureSet finalActiveFeatures = activeFeatures;

        checkActiveFeaturesInitialized(finalActiveFeatures);

        return finalActiveFeatures;
    }

    /** @return The feature set corresponding to the previous version under which this node operated. */
    @Nullable public IgniteNodeFeatureSet previousActiveFeatures() {
        return prevActiveFeatures;
    }

    /** @return {@code true} if the specified {@link IgniteFeature} is active in the cluster; {@code false} otherwise. */
    public boolean isActive(IgniteFeature feature) {
        final IgniteNodeFeatureSet finalActiveFeatures = activeFeatures;

        checkActiveFeaturesInitialized(finalActiveFeatures);

        return finalActiveFeatures.contains(feature);
    }

    /** Registers the specified listener to be notified when the {@link IgniteFeature} is activated. */
    public void listenActivation(IgniteFeature feature, IgniteRunnable lsnr) {
        assert locVerFeatures.contains(feature);

        final IgniteNodeFeatureSet finalActiveFeatures = activeFeatures;

        checkActiveFeaturesInitialized(finalActiveFeatures);

        if (finalActiveFeatures.contains(feature))
            lsnr.run();
        else
            locVerFeaturesActivationFut.listen(lsnr);
    }

    /** */
    public void onGridDataReceived(RollingUpgradeClusterData clusterData) {
        IgniteNodeFeatureSet activeClusterFeatures = clusterData.activeFeatures();

        boolean hasSameFeaturesAsCluster = ctx.clientNode()
            ? activeClusterFeatures.containsAll(locVerFeatures)
            : locVerFeatures.equals(activeClusterFeatures);

        if (hasSameFeaturesAsCluster) {
            prevActiveFeatures = clusterData.previousActiveFeatures();

            activateLocalVersionFeatures();
        }
        else {
            activeFeatures = activeClusterFeatures;
            prevActiveFeatures = activeClusterFeatures;
        }
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

    /** */
    private void checkActiveFeaturesInitialized(IgniteNodeFeatureSet activeFeatures) {
        if (activeFeatures == null) {
            throw new IllegalStateException("Local node features are not yet initialized [locNodeId=" +
                ctx.discovery().localNode().id() + ']');
        }
    }

    /** */
    private IgniteNodeFeatureSet collectLocalVersionFeatures(GridKernalContext ctx, IgniteCoreFeatureSet coreFeatures) {
        Collection<IgniteComponentFeatureSet> features = new ArrayList<>();

        features.add(coreFeatures);

        IgniteComponentFeatureSetProvider[] components = ctx.plugins().extensions(IgniteComponentFeatureSetProvider.class);

        if (!F.isEmpty(components)) {
            for (IgniteComponentFeatureSetProvider component : components)
                features.add(buildPluginFeatureSet(component));
        }

        return new IgniteNodeFeatureSet(features.toArray(IgniteComponentFeatureSet[]::new));
    }

    /** */
    private IgniteComponentFeatureSet buildPluginFeatureSet(IgniteComponentFeatureSetProvider cmpFeaturesProvider) {
        Collection<IgniteFeature> cmpFeatures = cmpFeaturesProvider.features();

        A.notEmpty(cmpFeatures, "component features");

        boolean allFeaturesBelongToComponent = cmpFeatures.stream()
            .map(IgniteFeature::componentName)
            .allMatch(featureCmp -> featureCmp.equals(cmpFeaturesProvider.componentName()));

        if (!allFeaturesBelongToComponent) {
            throw new IgniteException("All specified Ignite Features must belong to the same component" +
                " [componentName=" + cmpFeaturesProvider.componentName() + ']');
        }

        return new IgnitePluginFeatureSet(
            cmpFeaturesProvider.componentName(),
            cmpFeaturesProvider.componentVersion(),
            IgniteFeatureSet.buildFrom(cmpFeatures)
        );
    }
}
