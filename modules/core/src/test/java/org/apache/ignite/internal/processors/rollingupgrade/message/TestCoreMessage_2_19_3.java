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
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_19_3;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_19_3.VER_2_19_2_ID_2_FEATURE;

/** */
@FeatureGated(registry = TestIgniteReleaseFeatures_2_19_3.class)
public class TestCoreMessage_2_19_3 extends TestDiscoveryMessage {
    /** */
    @Order(0)
    String fldA;

    /** */
    @Order(1)
    String fldB;

    /** */
    @Order(value = 2, deprecatedBy = "VER_2_19_2_ID_2_FEATURE")
    String fldC;

    /** */
    @Order(value = 3, introducedBy = "VER_2_19_2_ID_1_FEATURE")
    String fldD;

    /** */
    @Order(value = 4, introducedBy = "VER_2_19_3_ID_6_FEATURE")
    String fldF;

    /** {@inheritDoc} */
    @Override public TestDiscoveryMessage fill(Predicate<IgniteFeature> featureStatusProvider) {
        fldA = A;
        fldB = B;

        if (!featureStatusProvider.test(VER_2_19_2_ID_2_FEATURE))
            fldC = C;

        fldD = D;
        fldF = F;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String fldA() {
        return fldA;
    }

    /** {@inheritDoc} */
    @Override public String fldB() {
        return fldB;
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
    @Override public String fldF() {
        return fldF;
    }
}
