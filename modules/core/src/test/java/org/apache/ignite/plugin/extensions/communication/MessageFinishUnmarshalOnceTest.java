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

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies the no-double-unmarshal check itself, not its coverage: {@link MessageMarshaller.Dedup#firstUnmarshal} must
 * detect a second finish-unmarshal of the same instance, and the check must be enabled for every test — otherwise the
 * suite-wide guard would silently turn off and pass every test vacuously. The actual coverage (no real receive path
 * unmarshals an instance twice) comes from running the check across the whole suite via {@code GridAbstractTest}.
 */
public class MessageFinishUnmarshalOnceTest extends GridCommonAbstractTest {
    /** The suite-wide guard must be on, so a silently-disabled check cannot pass every test without verifying anything. */
    @Test
    public void testCheckEnabled() {
        assertTrue("IGNITE_MESSAGE_UNMARSHAL_ONCE_CHECK must be set for every test by GridAbstractTest",
            MessageMarshaller.Dedup.ENABLED);
    }

    /** A second finish-unmarshal of the same instance must be detected; the first must be allowed. */
    @Test
    public void testSecondUnmarshalDetected() {
        MarshallableMessage msg = new NoopMarshallableMessage();

        assertTrue("First finish-unmarshal must be allowed", MessageMarshaller.Dedup.firstUnmarshal(msg));
        assertFalse("Second finish-unmarshal of the same instance must be detected",
            MessageMarshaller.Dedup.firstUnmarshal(msg));
    }

    /** Minimal {@link MarshallableMessage}; only its identity matters to the check. */
    private static class NoopMarshallableMessage implements MarshallableMessage {
        /** {@inheritDoc} */
        @Override public void prepareMarshal(Marshaller marsh) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
            // No-op.
        }
    }
}
