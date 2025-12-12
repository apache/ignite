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

import org.apache.ignite.plugin.extensions.communication.mappers.DefaultEnumMapper;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Verifies behavior of {@link DefaultEnumMapper} on a single enum type.
 */
public class DefaultEnumMapperTest {
    /** */
    private final TransactionIsolation[] txIsolationVals = TransactionIsolation.values();

    /** */
    @Test
    public void testEncode() {
        assertEquals(-1, DefaultEnumMapper.INSTANCE.encode(null));

        for (TransactionIsolation txIsolation : txIsolationVals)
            assertEquals(txIsolation.ordinal(), DefaultEnumMapper.INSTANCE.encode(txIsolation));
    }

    /** */
    @Test
    public void testDecode() {
        assertNull(DefaultEnumMapper.INSTANCE.decode(txIsolationVals, (byte)-1));

        for (TransactionIsolation txIsolation : txIsolationVals) {
            assertEquals(txIsolationVals[txIsolation.ordinal()],
                DefaultEnumMapper.INSTANCE.decode(txIsolationVals, (byte)txIsolation.ordinal()));
        }

        Throwable ex = GridTestUtils.assertThrowsWithCause(
            () -> DefaultEnumMapper.INSTANCE.decode(txIsolationVals, (byte)txIsolationVals.length),
            IllegalArgumentException.class
        );

        assertEquals("Enum code " + txIsolationVals.length + " is out of range for enum type " +
                TransactionIsolation.class.getName(),
            ex.getMessage());
    }
}
