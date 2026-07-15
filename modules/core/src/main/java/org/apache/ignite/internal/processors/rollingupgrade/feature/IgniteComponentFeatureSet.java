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
import java.util.Objects;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Represents a set of {@link IgniteFeature}s available for the specific Ignite component version. */
public abstract class IgniteComponentFeatureSet implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    IgniteProductVersion ver;

    /** */
    @Order(1)
    IgniteFeatureSet features;

    /** */
    public IgniteComponentFeatureSet() {
        // No-op.
    }

    /** */
    protected IgniteComponentFeatureSet(IgniteProductVersion ver, IgniteFeatureSet features) {
        A.notNull(ver, "component version");
        A.notNull(features, "component features");

        this.ver = ver;
        this.features = features;
    }

    /** */
    public abstract String componentName();

    /** */
    public IgniteProductVersion version() {
        return ver;
    }

    /** */
    public boolean contains(int featureId) {
        return features.contains(featureId);
    }

    /** */
    public boolean isUpgradableTo(IgniteComponentFeatureSet target) {
        return Objects.equals(componentName(), target.componentName()) && features.isUpgradableTo(target.features);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        ver.writeExternal(out);
        features.writeExternal(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ver = new IgniteProductVersion();
        ver.readExternal(in);

        features = new IgniteFeatureSet();
        features.readExternal(in);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        IgniteComponentFeatureSet other = (IgniteComponentFeatureSet)o;

        return Objects.equals(componentName(), other.componentName())
            && Objects.equals(features, other.features)
            && Objects.equals(ver, other.ver);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteComponent [name=" + componentName() + ", ver=" + ver + ']';
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(componentName(), features, ver);
    }
}
