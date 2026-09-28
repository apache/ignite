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


package org.apache.ignite.internal.processors.rollingupgrade.message;

import java.util.function.Predicate;
import org.apache.ignite.internal.FeatureGated;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestPluginReleaseFeatures_3_0_0;

/** */
@FeatureGated(registry = TestPluginReleaseFeatures_3_0_0.class)
public class TestPluginMessage_3_0_0 extends TestDiscoveryMessage {
    /** */
    @Order(0)
    String fldA;

    /** */
    @Order(1)
    String fldC;

    /** */
    @Order(2)
    String fldD;

    /** */
    @Order(value = 3, introducedBy = "VER_2_0_0_ID_2_FEATURE")
    String fldE;

    /** */
    @Order(value = 4, introducedBy = "VER_2_1_0_ID_3_FEATURE")
    String fldF;

    /** {@inheritDoc} */
    @Override public TestDiscoveryMessage fill(Predicate<IgniteFeature> featureStatusProvider) {
        fldA = A;
        fldC = C;
        fldD = D;
        fldE = E;
        fldF = F;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String fldA() {
        return fldA;
    }

    /** {@inheritDoc} */
    @Override public String fldC() {
        return fldC;
    }

    /** {@inheritDoc} */
    @Override public String fldD() {
        return fldD;
    }

    /** {@inheritDoc} */
    @Override public String fldE() {
        return fldE;
    }

    /** {@inheritDoc} */
    @Override public String fldF() {
        return fldF;
    }
}
