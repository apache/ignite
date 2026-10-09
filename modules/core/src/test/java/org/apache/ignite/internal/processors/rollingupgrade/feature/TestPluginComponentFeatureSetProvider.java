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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.lang.IgniteProductVersion;

/** */
public class TestPluginComponentFeatureSetProvider implements IgniteComponentFeatureSetProvider {
    /** */
    private final String pluginVer;

    /** */
    public TestPluginComponentFeatureSetProvider(String pluginVer) {
        this.pluginVer = pluginVer;
    }

    /** {@inheritDoc} */
    @Override public String componentName() {
        return TestPluginFeature.COMPONENT_NAME;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion componentVersion() {
        return IgniteProductVersion.fromString(pluginVer);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFeature> features() {
        try {
            return AbstractRollingUpgradeTest.readDeclaredPluginFeatures(pluginVer);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }
}
