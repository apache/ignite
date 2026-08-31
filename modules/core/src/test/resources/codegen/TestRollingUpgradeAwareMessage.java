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

package org.apache.ignite.internal;

import org.apache.ignite.plugin.extensions.communication.Message;

/** */
@FeatureRegistry(TestFeatureRegistry.class)
public class TestRollingUpgradeAwareMessage implements Message {
    /** Unconditional field. */
    @Order(0)
    int plain;

    /** Written only while the feature is not agreed. */
    @Order(value = 1, deprecatedBy = "FIRST_FEATURE")
    String oldFld;

    /** Written only once the feature is agreed. */
    @Order(value = 2, introducedBy = "FIRST_FEATURE")
    String newFld;

    /** Lived for one release window only. */
    @Order(value = 3, introducedBy = "FIRST_FEATURE", deprecatedBy = "SECOND_FEATURE")
    long windowed;

    /** {@inheritDoc} */
    @Override public short directType() {
        return 0;
    }
}
