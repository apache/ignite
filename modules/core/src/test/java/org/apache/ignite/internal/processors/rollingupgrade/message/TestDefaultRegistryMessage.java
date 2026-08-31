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

import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public class TestDefaultRegistryMessage implements Message, TestMessage {
    /** */
    @Order(0)
    String fldA;

    /** */
    @Order(value = 1, deprecatedBy = "ROLLING_UPGRADE_FEATURE")
    String fldB;

    /** */
    @Order(2)
    String fldC;

    /** */
    @Order(value = 3, introducedBy = "ROLLING_UPGRADE_FEATURE")
    String fldD;

    /** */
    public static TestDefaultRegistryMessage build() {
        TestDefaultRegistryMessage msg = new TestDefaultRegistryMessage();

        msg.fldA = A;
        msg.fldB = B;
        msg.fldC = C;
        msg.fldD = D;

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
        return null;
    }

    /** {@inheritDoc} */
    @Override public String fldF() {
        return null;
    }
}
