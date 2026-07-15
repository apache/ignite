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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Represents the set of {@link IgniteFeature}s associated with a user plugin.
 * Explicitly stores the name of the plugin to which the feature set belongs.
 */
public class IgnitePluginFeatureSet extends IgniteComponentFeatureSet {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    String compName;

    /** */
    public IgnitePluginFeatureSet() {
        // No-op.
    }

    /** */
    public IgnitePluginFeatureSet(String compName, IgniteProductVersion compVer, IgniteFeatureSet features) {
        super(compVer, features);

        A.notNull(compName, "component name");

        this.compName = compName;
    }

    /** */
    public String componentName() {
        return compName;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(compName);

        super.writeExternal(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        compName = in.readUTF();

        super.readExternal(in);
    }
}
