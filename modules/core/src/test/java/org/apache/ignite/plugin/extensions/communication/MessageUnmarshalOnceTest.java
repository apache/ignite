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

import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.managers.communication.MessageUnmarshalOnceCheck;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies the no-double-unmarshal check itself, not its coverage: {@link MessageUnmarshalOnceCheck#firstUnmarshal} must
 * detect a second finish-unmarshal of the same instance within one pass, while allowing the two legitimate passes
 * (cache-free and cache-aware), and the check must be enabled for every test — otherwise the suite-wide guard would
 * silently turn off and pass every test vacuously. The actual coverage (no real receive path unmarshals an instance
 * twice in the same pass) comes from running the check across the whole suite via {@code GridAbstractTest}.
 */
public class MessageUnmarshalOnceTest extends GridCommonAbstractTest {
    /** The suite-wide guard must be on, so a silently-disabled check cannot pass every test without verifying anything. */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        assertTrue("IGNITE_MESSAGE_UNMARSHAL_ONCE_CHECK must be set for every test by GridAbstractTest",
            MessageUnmarshalOnceCheck.ENABLED);
    }

    /** A second finish-unmarshal of the same instance within one pass must be detected; the first must be allowed. */
    @Test
    public void testSecondUnmarshalDetected() {
        MarshallableMessage msg = new NoopMarshallableMessage();

        assertTrue("First finish-unmarshal must be allowed", MessageUnmarshalOnceCheck.firstUnmarshal(msg, false));
        assertFalse("Second finish-unmarshal of the same instance in the same pass must be detected",
            MessageUnmarshalOnceCheck.firstUnmarshal(msg, false));
    }

    /** The two legitimate passes (cache-free and cache-aware) over one instance must both be allowed. */
    @Test
    public void testBothPassesAllowed() {
        MarshallableMessage msg = new NoopMarshallableMessage();

        assertTrue("Cache-free pass must be allowed", MessageUnmarshalOnceCheck.firstUnmarshal(msg, false));
        assertTrue("Cache-aware pass over the same instance must also be allowed",
            MessageUnmarshalOnceCheck.firstUnmarshal(msg, true));
    }

    /** Minimal {@link MarshallableMessage}; only its identity matters to the check. */
    private static class NoopMarshallableMessage implements MarshallableMessage {
        /** {@inheritDoc} */
        @Override public void marshal(Marshaller marsh) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) {
            // No-op.
        }
    }
}
