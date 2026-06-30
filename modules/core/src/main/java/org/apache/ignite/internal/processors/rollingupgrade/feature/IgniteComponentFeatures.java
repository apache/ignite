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
public class IgniteComponentFeatures implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    String compName;

    /** */
    @Order(1)
    IgniteProductVersion compVer;

    /** */
    @Order(2)
    IgniteFeatureSet features;

    /** */
    public IgniteComponentFeatures() {
        // No-op.
    }

    /** */
    public IgniteComponentFeatures(String compName, IgniteProductVersion compVer, IgniteFeatureSet features) {
        A.notNull(compName, "component name");
        A.notNull(compVer, "component version");
        A.notNull(features, "component features");

        this.compName = compName;
        this.compVer = compVer;
        this.features = features;
    }

    /** */
    public String componentName() {
        return compName;
    }

    /** */
    public IgniteProductVersion version() {
        return compVer;
    }

    /** */
    public boolean contains(int featureId) {
        return features.contains(featureId);
    }

    /** */
    public boolean isUpgradableTo(IgniteComponentFeatures target) {
        return Objects.equals(compName, target.compName) && features.isUpgradableTo(target.features);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(compName);
        out.writeObject(compVer);
        out.writeObject(features);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        compName = in.readUTF();
        compVer = (IgniteProductVersion)in.readObject();
        features = (IgniteFeatureSet)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        IgniteComponentFeatures other = (IgniteComponentFeatures)o;

        return Objects.equals(compName, other.compName)
            && Objects.equals(features, other.features)
            && Objects.equals(compVer, other.compVer);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteComponent [name=" + compName + ", ver=" + compVer + ']';
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(compName, features, compVer);
    }
}
