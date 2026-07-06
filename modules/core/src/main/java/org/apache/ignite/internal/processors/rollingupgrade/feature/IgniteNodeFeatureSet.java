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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a set of {@link IgniteFeature}s supported by an Ignite node. Ignite is divided into independent components.
 * Each component is associated with its version and a set of {@link IgniteFeature}s.
 */
public class IgniteNodeFeatureSet {
    /** */
    @GridToStringExclude
    private final Map<String, IgniteComponentFeatureSet> features;

    /** */
    public IgniteNodeFeatureSet(Collection<IgniteComponentFeatureSet> features) {
        this.features = indexByComponentName(features);
    }

    /** */
    public Set<String> components() {
        return Collections.unmodifiableSet(features.keySet());
    }

    /** */
    public Collection<IgniteComponentFeatureSet> values() {
        return Collections.unmodifiableCollection(features.values());
    }

    /** */
    @Nullable public IgniteComponentFeatureSet componentFeatures(String cmpName) {
        return features.get(cmpName);
    }

    /** */
    public boolean containsAll(IgniteNodeFeatureSet other) {
        if (!components().containsAll(other.components()))
            return false;

        for (IgniteComponentFeatureSet otherCmpFeatures : other.features.values()) {
            if (!otherCmpFeatures.equals(features.get(otherCmpFeatures.componentName())))
                return false;
        }

        return true;
    }

    /** */
    public boolean contains(IgniteFeature feature) {
        IgniteComponentFeatureSet cmpFeatures = features.get(feature.componentName());

        return cmpFeatures != null && cmpFeatures.contains(feature.id());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        IgniteNodeFeatureSet other = (IgniteNodeFeatureSet)o;

        return Objects.equals(features, other.features);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hashCode(features);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return features.values().stream().map(IgniteComponentFeatureSet::toString).collect(Collectors.joining(", ", "[", "]"));
    }

    /** */
    private static Map<String, IgniteComponentFeatureSet> indexByComponentName(Collection<IgniteComponentFeatureSet> features) {
        Map<String, IgniteComponentFeatureSet> res = new HashMap<>();

        for (IgniteComponentFeatureSet compFeatures : features) {
            if (res.put(compFeatures.componentName(), compFeatures) != null)
                throw new IgniteException("Duplicated component name [cmpName=" + compFeatures.componentName() + ']');
        }

        return res;
    }
}
