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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class DeploymentModeMessageTest {
    /** */
    @Test
    public void testDeploymentModeCode() {
        assertEquals(-1, new DeploymentModeMessage(null).code());
        assertEquals(0, new DeploymentModeMessage(PRIVATE).code());
        assertEquals(1, new DeploymentModeMessage(ISOLATED).code());
        assertEquals(2, new DeploymentModeMessage(SHARED).code());
        assertEquals(3, new DeploymentModeMessage(CONTINUOUS).code());

        for (DeploymentMode depMode : DeploymentMode.values()) {
            assertTrue(new DeploymentModeMessage(depMode).code() >= 0);
            assertTrue(new DeploymentModeMessage(depMode).code() < 4);
        }
    }

    /** */
    @Test
    public void testDeploymentModeFromCode() {
        DeploymentModeMessage msg = new DeploymentModeMessage(null);

        msg.code((byte)-1);
        assertNull(msg.value());

        msg.code((byte)0);
        assertSame(PRIVATE, msg.value());

        msg.code((byte)1);
        assertSame(ISOLATED, msg.value());

        msg.code((byte)2);
        assertSame(SHARED, msg.value());

        msg.code((byte)3);
        assertSame(CONTINUOUS, msg.value());

        Throwable t = assertThrowsWithCause(() -> msg.code((byte)4), IllegalArgumentException.class);
        assertEquals("Unknown deployment mode code: 4", t.getMessage());
    }

    /** */
    @Test
    public void testConversionConsistency() {
        for (DeploymentMode depMode : F.concat(DeploymentMode.values(), (DeploymentMode)null)) {
            DeploymentModeMessage msg = new DeploymentModeMessage(depMode);

            assertEquals(depMode, msg.value());

            DeploymentModeMessage newMsg = new DeploymentModeMessage();
            newMsg.code(msg.code());

            assertEquals(msg.value(), newMsg.value());
        }
    }
}
