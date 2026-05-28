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

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.lang.IgniteProductVersion;

/** Represents a set of {@link IgniteFeature}s available for the specific Ignite product version. */
public class IgniteProductFeatures implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteProductVersion ver;

    /** */
    private final IgniteFeatureSet features;

    /** */
    public IgniteProductFeatures(IgniteProductVersion ver, IgniteFeatureSet features) {
        this.ver = ver;
        this.features = features;
    }

    /** */
    public IgniteProductVersion version() {
        return ver;
    }

    /** */
    public IgniteFeatureSet features() {
        return features;
    }

    /** */
    public boolean contains(IgniteFeature feature) {
        return features.contains(feature.id());
    }

    /** */
    public boolean isUpgradableTo(IgniteProductFeatures target) {
        return features.isUpgradableTo(target.features);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        IgniteProductFeatures other = (IgniteProductFeatures)o;

        return Objects.equals(features, other.features) && Objects.equals(ver, other.ver);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(features, ver);
    }
}
