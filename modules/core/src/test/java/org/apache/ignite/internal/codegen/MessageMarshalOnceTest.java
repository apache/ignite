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

package org.apache.ignite.internal.codegen;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecordMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Guards against a marshalling performance regression. The codegen wraps each {@code @Marshalled} field's
 * {@code U.marshal} in a {@code wireField == null} guard, so a message {@code prepareMarshal}'d N times (broadcast to
 * N nodes) marshals each field at most once: the wire bytes are computed on the first call and reused thereafter.
 * Verified behaviourally — the wire array is the same reference after a second {@code prepareMarshal}, independent of
 * how the guard is formatted (a re-marshal would create a new array).
 */
public class MessageMarshalOnceTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPrepareMarshalMarshalsOnce() throws Exception {
        GridKernalContext kctx = startGrid(0).context();

        // One @Marshalled message is representative: the marshal-once guard comes from one generator template
        // (only field names vary), so a regression in it breaks every such message identically.
        TransactionsHashRecord msg = new TransactionsHashRecord("local", "remote", 0);

        TransactionsHashRecordMarshaller marshaller = new TransactionsHashRecordMarshaller(kctx.marshaller());

        marshaller.prepareMarshal(msg, kctx, null);

        byte[] first = GridTestUtils.getFieldValue(msg, "locConsistentIdBytes");

        assertNotNull("First prepareMarshal must marshal the field", first);

        marshaller.prepareMarshal(msg, kctx, null);

        byte[] second = GridTestUtils.getFieldValue(msg, "locConsistentIdBytes");

        assertTrue("Second prepareMarshal re-marshalled the field — missing 'wire-field == null' guard", first == second);
    }
}
