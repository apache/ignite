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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.cdc.TypeMappingImpl;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Metadata registration test.
 *
 * @see TypeMapping
 * @see CdcConsumer
 */
public class MetadataRegistrationTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** @throws Exception If failed. */
    @Test
    public void testBinaryMeta() throws Exception {
        IgniteEx srv = startGrid();

        IgniteBinary srvBinary = srv.binary();

        try (IgniteClient client = startClient(srv)) {
            BinaryMetadata meta = new BinaryMetadata(123, "newType", null, null, false, null);

            assertNull(srvBinary.type(meta.typeId()));

            BinaryContext ctx = ((ClientBinary)client.binary()).binaryContext();

            ctx.updateMetadata(meta.typeId(), meta, false);

            assertNotNull(srvBinary.type(meta.typeId()));
            assertEquals(meta.typeName(), srvBinary.type(meta.typeId()).typeName());
        }
    }

    /** @throws Exception If failed. */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testMapping() throws Exception {
        IgniteEx srv = startGrid();

        MarshallerContextImpl marshCtx = srv.context().marshallerContext();

        try (IgniteClient client = startClient(srv)) {
            TypeMapping mapping = new TypeMappingImpl(123, "newType", PlatformType.JAVA);

            byte platformType = (byte)mapping.platformType().ordinal();

            GridTestUtils.assertThrowsWithCause(() -> marshCtx.getClassName(platformType, mapping.typeId()),
                ClassNotFoundException.class);

            BinaryContext ctx = ((ClientBinary)client.binary()).binaryContext();

            ctx.registerUserClassName(mapping.typeId(), mapping.typeName(), false, false, platformType);

            assertEquals(mapping.typeName(), marshCtx.getClassName(platformType, mapping.typeId()));
        }
    }
}
