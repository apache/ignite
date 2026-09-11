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

import org.apache.ignite.internal.FeatureRegistry;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestPluginReleaseFeatures_2_1_0;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/** */
@FeatureRegistry(TestPluginReleaseFeatures_2_1_0.class)
public class TestPluginMessage extends DiscoveryCustomMessage implements TestMessage {
    /** */
    @Order(0)
    String fldA;

    /** */
    @Order(value = 1, deprecatedBy = "VER_2_0_0_ID_1_FEATURE")
    String fldB;

    /** */
    @Order(2)
    String fldC;

    /** */
    @Order(value = 3, introducedBy = "VER_1_0_0_ID_0_FEATURE", deprecatedBy = "VER_2_0_0_ID_1_FEATURE")
    String fldD;

    /** */
    @Order(value = 4, introducedBy = "VER_2_0_0_ID_1_FEATURE")
    String fldE;

    /** */
    @Order(value = 5, introducedBy = "VER_2_1_0_ID_2_FEATURE")
    String fldF;

    /** */
    public TestPluginMessage() {
        super(IgniteUuid.randomUuid());
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** */
    public static TestPluginMessage build() {
        TestPluginMessage msg = new TestPluginMessage();

        msg.fldA = A;
        msg.fldB = B;
        msg.fldC = C;
        msg.fldD = D;
        msg.fldE = E;
        msg.fldF = F;

        return msg;
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
    @Override public String fldE() {
        return fldE;
    }

    /** {@inheritDoc} */
    @Override public String fldF() {
        return fldF;
    }
}
