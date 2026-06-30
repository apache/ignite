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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

/** Maintains the set of active cluster {@link IgniteComponentFeatures} used by Rolling Upgrade logic. */
public class IgniteFeatureManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteFeatures locVerFeatures;

    /** */
    private final GridFutureAdapter<Void> locVerFeaturesActivationFut;

    /** */
    private volatile IgniteFeatures activeFeatures;

    /** */
    public IgniteFeatureManager(GridKernalContext ctx, IgniteComponentFeatures locCoreFeatures) {
        this.ctx = ctx;
        this.locVerFeatures = collectLocalVersionFeatures(ctx, locCoreFeatures);
        locVerFeaturesActivationFut = new GridFutureAdapter<>();
    }

    /** @return The set of features declared by the local node's product version. */
    public IgniteFeatures localVersionFeatures() {
        return locVerFeatures;
    }

    /** @return Active functions of the specified component. */
    @Nullable public IgniteComponentFeatures activeComponentFeatures(String cmpName) {
        return activeFeatures().componentFeatures(cmpName);
    }

    /** @return The set of features currently active in the cluster. */
    public IgniteFeatures activeFeatures() {
        final IgniteFeatures finalActiveFeatures = activeFeatures;

        checkActiveFeaturesInitialized(finalActiveFeatures);

        return finalActiveFeatures;
    }

    /** @return {@code true} if the specified {@link IgniteFeature} is active in the cluster; {@code false} otherwise. */
    public boolean isActive(IgniteFeature feature) {
        final IgniteFeatures finalActiveFeatures = activeFeatures;

        checkActiveFeaturesInitialized(finalActiveFeatures);

        return finalActiveFeatures.contains(feature);
    }

    /** Registers the specified listener to be notified when the {@link IgniteFeature} is activated. */
    public void listenActivation(IgniteFeature feature, IgniteRunnable lsnr) {
        assert locVerFeatures.contains(feature);

        final IgniteFeatures finalActiveFeatures = activeFeatures;

        checkActiveFeaturesInitialized(finalActiveFeatures);

        if (finalActiveFeatures.contains(feature))
            lsnr.run();
        else
            locVerFeaturesActivationFut.listen(lsnr);
    }

    /** */
    public void onGridDataReceived(IgniteFeatures activeClusterFeatures) {
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

    /** */
    private void checkActiveFeaturesInitialized(IgniteFeatures activeFeatures) {
        if (activeFeatures == null) {
            throw new IllegalStateException("Local node features are not yet initialized [locNodeId=" +
                ctx.discovery().localNode().id() + ']');
        }
    }

    /** */
    private IgniteFeatures collectLocalVersionFeatures(GridKernalContext ctx, IgniteComponentFeatures locCoreFeatures) {
        Set<IgniteComponentFeatures> features = new HashSet<>();

        features.add(locCoreFeatures);

        IgniteComponentFeaturesProvider[] components = ctx.plugins().extensions(IgniteComponentFeaturesProvider.class);

        if (!F.isEmpty(components)) {
            for (IgniteComponentFeaturesProvider component : components)
                features.add(buildComponentFeatures(component));
        }

        return new IgniteFeatures(features);
    }

    /** */
    private IgniteComponentFeatures buildComponentFeatures(IgniteComponentFeaturesProvider cmpFeaturesProvider) {
        Collection<IgniteFeature> cmpFeatures = cmpFeaturesProvider.features();

        A.notEmpty(cmpFeatures, "component features");

        boolean allFeaturesBelongToComponent = cmpFeatures.stream()
            .map(IgniteFeature::componentName)
            .allMatch(featureCmp -> featureCmp.equals(cmpFeaturesProvider.componentName()));

        if (!allFeaturesBelongToComponent) {
            throw new IgniteException("All specified Ignite Features must belong to the current component" +
                " [componentName=" + cmpFeaturesProvider.componentName() + ']');
        }

        return new IgniteComponentFeatures(
            cmpFeaturesProvider.componentName(),
            cmpFeaturesProvider.componentVersion(),
            IgniteFeatureSet.buildFrom(cmpFeatures)
        );
    }
}
