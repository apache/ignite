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

import org.apache.ignite.internal.DeprecatedBy;
import org.apache.ignite.internal.IntroducedBy;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestPluginReleaseFeatures_1_0_0;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestPluginReleaseFeatures_2_0_0;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestPluginReleaseFeatures_2_1_0;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Carries fields guarded by features of two components at once: the core one and the test plugin's.
 * <p>
 * Only exchanged between nodes that declare the plugin component. A field guarded by a feature of a
 * component neither node declares has no answer - see {@code IgniteMessageSerializationContext#component} -
 * so this message and the core-only {@link TestCoreMessage} are kept apart rather than
 * folded into one that every test would have to declare the plugin for.
 */
public class TestPluginMessage implements Message, TestMessage {
    /** */
    @Order(0)
    String fldA;

    /** */
    @Order(1)
    @DeprecatedBy(value = "VER_2_0_0_ID_1_FEATURE", registry = TestPluginReleaseFeatures_2_0_0.class)
    String fldB;

    /** */
    @Order(2)
    String fldC;

    /** */
    @Order(3)
    @IntroducedBy(value = "VER_1_0_0_ID_0_FEATURE", registry = TestPluginReleaseFeatures_1_0_0.class)
    @DeprecatedBy(value = "VER_2_0_0_ID_1_FEATURE", registry = TestPluginReleaseFeatures_2_0_0.class)
    String fldD;

    /** */
    @Order(4)
    @IntroducedBy(value = "VER_2_0_0_ID_1_FEATURE", registry = TestPluginReleaseFeatures_2_0_0.class)
    String fldE;

    /** */
    @Order(5)
    @IntroducedBy(value = "VER_2_1_0_ID_2_FEATURE", registry = TestPluginReleaseFeatures_2_1_0.class)
    String fldF;

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
