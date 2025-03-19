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

package org.apache.ignite.internal.binary;

import java.util.List;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryReflectiveSerializer;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;


/**
 *
 */
public class BinaryContextUserDefinedTypesTest extends GridCommonAbstractTest {
    /**
     * Tests that user defined types have get the correct serializer.
     */
    @Test
    public void testDefaultConstructor() {
        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        List<BinaryTypeConfiguration> binaryTypeCfgs = List.of(
            new BinaryTypeConfiguration(UserDefinedTypeWithBinarySerializer.class.getCanonicalName()).setSerializer(new BinaryReflectiveSerializer()),
            new BinaryTypeConfiguration(UserDefinedTypeWithBinarySerializer.class.getCanonicalName()).setSerializer(new BinaryReflectiveSerializer())
        );
        igniteCfg.setBinaryConfiguration(new BinaryConfiguration()
            .setSerializer(new DefaultSerializer())
            .setTypeConfigurations(binaryTypeCfgs)
        );

        BinaryContext binCtx = new BinaryContext(BinaryNoopMetadataHandler.instance(), igniteCfg, log);

        // Validate that if a serializer is provided in the configuration, it is used instead of the default one.
        // TODO(taouad): test the case with .*
        assertEquals(BinaryReflectiveSerializer.class, binCtx.serializerForClass(UserDefinedTypeWithDefaultSerializer.class));
        assertEquals(DefaultSerializer.class, binCtx.serializerForClass(UserDefinedTypeWithDefaultSerializer.class));
    }

    static class UserDefinedTypeWithDefaultSerializer {
    }

    static class UserDefinedTypeWithBinarySerializer {
    }

    private static class DefaultSerializer implements BinarySerializer {
        @Override
        public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
        }

        @Override
        public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
        }
    }
}
