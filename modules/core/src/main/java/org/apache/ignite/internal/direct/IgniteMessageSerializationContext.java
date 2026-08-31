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

package org.apache.ignite.internal.direct;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.MessageSerializationContext;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteComponentFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteNodeFeatureSet;
import org.apache.ignite.spi.discovery.tcp.internal.UnsupportedNodeVersionException;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteMessageSerializationContext implements MessageSerializationContext {
    /** */
    private final Map<String, ComponentMessageSerializationContext> ctxByComponent;

    /** */
    private IgniteMessageSerializationContext(Map<String, ComponentMessageSerializationContext> ctxByComponent) {
        this.ctxByComponent = ctxByComponent;
    }

    /** {@inheritDoc} */
    @Override public boolean includeFieldIntroducedBy(IgniteFeature feature) {
        return componentContext(feature).includeFieldIntroducedBy(feature.id());
    }

    /** {@inheritDoc} */
    @Override public boolean includeFieldDeprecatedBy(IgniteFeature feature) {
        return componentContext(feature).includeFieldDeprecatedBy(feature.id());
    }

    /** */
    private ComponentMessageSerializationContext componentContext(IgniteFeature feature) {
        ComponentMessageSerializationContext cmpCtx = ctxByComponent.get(feature.componentName());

        if (cmpCtx == null) {
            throw new IllegalStateException(
                "A field is guarded by a feature of an undeclared component. The component must register its messages " +
                    "and feature set [feature=" + feature + ", component=" + feature.componentName() +
                    ", declaredComponents=" + ctxByComponent.keySet() + ']'
            );
        }

        return cmpCtx;
    }

    /** */
    public static IgniteMessageSerializationContext buildForPeers(
        Ignite loc,
        ClusterNode rmt
    ) throws UnsupportedNodeVersionException, ClusterTopologyCheckedException {
        assert rmt instanceof IgniteClusterNode : rmt;

        return buildForPeers(loc, resolveRemoteFeatures((IgniteEx)loc, (IgniteClusterNode)rmt));
    }

    /** */
    public static IgniteMessageSerializationContext buildForPeers(
        Ignite loc,
        IgniteNodeFeatureSet rmt
    ) throws UnsupportedNodeVersionException {
        return buildForPeers(((IgniteEx)loc).context().localNodeFeatures(), rmt);
    }

    /** */
    public static IgniteMessageSerializationContext buildForPeers(
        IgniteNodeFeatureSet loc,
        IgniteNodeFeatureSet rmt
    ) throws UnsupportedNodeVersionException {
        assert loc != null;

        if (rmt == null) {
            throw new UnsupportedNodeVersionException("Failed to build the message serialization context for the remote node." +
                " The remote node's feature set is unavailable.");
        }

        Set<String> components = new HashSet<>(loc.components());

        components.addAll(rmt.components());

        Map<String, ComponentMessageSerializationContext> ctxByComponent = new HashMap<>();

        for (String cmp : components) {
            ComponentMessageSerializationContext ctx = resolveComponentSerializationContext(
                cmp,
                loc.componentFeatures(cmp),
                rmt.componentFeatures(cmp)
            );

            ctxByComponent.put(cmp, ctx);
        }

        return new IgniteMessageSerializationContext(ctxByComponent);
    }

    /** */
    private static @Nullable IgniteNodeFeatureSet resolveRemoteFeatures(
        IgniteEx loc,
        IgniteClusterNode rmt
    ) throws ClusterTopologyCheckedException {
        IgniteNodeFeatureSet features = rmt.features();

        if (features != null)
            return features;

        // Node features are unavailable when the node instance was created by Java deserialization
        // (see {@link GridAffinityAssignment}). In this case, resolve the node again from the
        // local discovery cache by its ID.
        ClusterNode cachedNode = loc.context().discovery().node(rmt.id());

        if (cachedNode == null) {
            throw new ClusterTopologyCheckedException(
                "Failed to resolve the remote node by ID because it has left the cluster [nodeId=" + rmt.id() + ']');
        }

        assert cachedNode instanceof IgniteClusterNode : cachedNode;

        return ((IgniteClusterNode)cachedNode).features();
    }

    /** */
    private static ComponentMessageSerializationContext resolveComponentSerializationContext(
        String cmpName,
        @Nullable IgniteComponentFeatureSet locCmpFeatures,
        @Nullable IgniteComponentFeatureSet rmtCmpFeatures
    ) throws UnsupportedNodeVersionException {
        assert locCmpFeatures != null || rmtCmpFeatures != null;

        if (locCmpFeatures == null || rmtCmpFeatures == null)
            return new ComponentMessageSerializationContext(null, null);

        int c = locCmpFeatures.version().compareTo(rmtCmpFeatures.version());

        if (c == 0) {
            assert locCmpFeatures.features().equals(rmtCmpFeatures.features());

            return new ComponentMessageSerializationContext(null, rmtCmpFeatures.features());
        }
        else {
            IgniteComponentFeatureSet src = c < 0 ? locCmpFeatures : rmtCmpFeatures;
            IgniteComponentFeatureSet target = c < 0 ? rmtCmpFeatures : locCmpFeatures;

            if (!src.isUpgradableTo(target)) {
                throw new UnsupportedNodeVersionException("Remote node component versions are not supported" +
                    " [component=" + cmpName +
                    ", locComponent=" + locCmpFeatures +
                    ", rmtComponent=" + rmtCmpFeatures + ']');
            }

            return new ComponentMessageSerializationContext(src.features(), src.features());
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteMessageSerializationContext " + ctxByComponent;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return Objects.equals(ctxByComponent, ((IgniteMessageSerializationContext)o).ctxByComponent);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hashCode(ctxByComponent);
    }

    /** */
    private static final class ComponentMessageSerializationContext {
        /** */
        @Nullable private final IgniteFeatureSet excludedDeprecatedFields;

        /** */
        @Nullable private final IgniteFeatureSet includedIntroducedFields;

        /** */
        private ComponentMessageSerializationContext(
            @Nullable IgniteFeatureSet excludedDeprecatedFields,
            @Nullable IgniteFeatureSet includedIntroducedFields
        ) {
            this.excludedDeprecatedFields = excludedDeprecatedFields;
            this.includedIntroducedFields = includedIntroducedFields;
        }

        /** */
        boolean includeFieldIntroducedBy(int featureId) {
            return includedIntroducedFields != null && includedIntroducedFields.contains(featureId);
        }

        /** */
        boolean includeFieldDeprecatedBy(int featureId) {
            return excludedDeprecatedFields == null || !excludedDeprecatedFields.contains(featureId);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ComponentMessageSerializationContext other = (ComponentMessageSerializationContext)o;

            return Objects.equals(excludedDeprecatedFields, other.excludedDeprecatedFields)
                && Objects.equals(includedIntroducedFields, other.includedIntroducedFields);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(includedIntroducedFields, excludedDeprecatedFields);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ComponentContext [introduced=" + includedIntroducedFields + ", deprecated=" + excludedDeprecatedFields + ']';
        }
    }
}
