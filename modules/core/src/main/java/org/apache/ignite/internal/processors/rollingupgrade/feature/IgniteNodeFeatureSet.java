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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a set of {@link IgniteFeature}s supported by an Ignite node. Ignite is divided into independent components.
 * Each component is associated with its version and a set of {@link IgniteFeature}s.
 */
public class IgniteNodeFeatureSet implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final IgniteNodeFeatureSet LOCAL_CORE_FEATURES = new IgniteNodeFeatureSet(new IgniteComponentFeatureSet[] {
        IgniteCoreFeatureSet.local()
    });

    /** */
    @Order(0)
    IgniteComponentFeatureSet[] features;

    /** */
    @Nullable private volatile Map<String, IgniteComponentFeatureSet> featuresByComponent;

    /** */
    public IgniteNodeFeatureSet() {
        // No-op.
    }

    /** */
    public IgniteNodeFeatureSet(IgniteComponentFeatureSet[] features) {
        assert features != null;

        this.features = features;
        this.featuresByComponent = indexByComponentName(features);
    }

    /** */
    public Set<String> components() {
        return Collections.unmodifiableSet(featuresByComponent().keySet());
    }

    /** */
    public IgniteComponentFeatureSet[] values() {
        return features;
    }

    /** */
    @Nullable public IgniteComponentFeatureSet componentFeatures(String cmpName) {
        return featuresByComponent().get(cmpName);
    }

    /** */
    public boolean containsAll(IgniteNodeFeatureSet other) {
        if (!components().containsAll(other.components()))
            return false;

        for (IgniteComponentFeatureSet otherCmpFeatures : other.features) {
            if (!otherCmpFeatures.equals(featuresByComponent().get(otherCmpFeatures.componentName())))
                return false;
        }

        return true;
    }

    /** */
    public boolean contains(IgniteFeature feature) {
        IgniteComponentFeatureSet cmpFeatures = featuresByComponent().get(feature.componentName());

        return cmpFeatures != null && cmpFeatures.contains(feature.id());
    }

    /** */
    private Map<String, IgniteComponentFeatureSet> featuresByComponent() {
        Map<String, IgniteComponentFeatureSet> featuresByComponent = this.featuresByComponent;

        if (featuresByComponent != null)
            return featuresByComponent;

        featuresByComponent = indexByComponentName(features);

        this.featuresByComponent = featuresByComponent;

        return featuresByComponent;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(features.length);

        for (IgniteComponentFeatureSet feature : features)
            out.writeObject(feature);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        features = new IgniteComponentFeatureSet[in.readInt()];

        for (int i = 0; i < features.length; i++)
            features[i] = (IgniteComponentFeatureSet)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        IgniteNodeFeatureSet other = (IgniteNodeFeatureSet)o;

        return Objects.equals(featuresByComponent(), other.featuresByComponent());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hashCode(featuresByComponent());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return Arrays.stream(features).map(IgniteComponentFeatureSet::toString).collect(Collectors.joining(", ", "[", "]"));
    }

    /** */
    private static Map<String, IgniteComponentFeatureSet> indexByComponentName(IgniteComponentFeatureSet[] features) {
        Map<String, IgniteComponentFeatureSet> res = new HashMap<>();

        for (IgniteComponentFeatureSet compFeatures : features) {
            if (res.put(compFeatures.componentName(), compFeatures) != null)
                throw new IgniteException("Duplicated component name [cmpName=" + compFeatures.componentName() + ']');
        }

        return res;
    }
}
