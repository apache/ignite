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
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.Extension;

/**
 * Provides the ability for internal components (e.g., plugins and SPIs) to provide their own independent sets of
 * {@link IgniteFeature}s that will be accounted during Rolling Upgrade.
 *
 * @see IgniteFeature
 */
public interface IgniteComponentFeatureSetProvider extends Extension {
    /** The name of the Ignite component that provides its own set of {@link IgniteFeature}s. */
    public String componentName();

    /** The version of the Ignite component with which the provided {@link IgniteFeature}s will be associated. */
    public IgniteProductVersion componentVersion();

    /**
     * The set of features supported by the Ignite component. Note that the {@link IgniteFeature#componentName()} value
     * for all features must match the {@link #componentName()} value.
     */
    public Collection<IgniteFeature> features();
}
