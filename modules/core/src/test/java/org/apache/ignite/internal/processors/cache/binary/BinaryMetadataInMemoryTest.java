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
package org.apache.ignite.internal.processors.cache.binary;

import java.io.File;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test binary metadata for in-memory cluster.
 */
public class BinaryMetadataInMemoryTest extends GridCommonAbstractTest {
    /**
     * Test that binary_meta directory is empty for in-memory cluster.
     */
    @Test
    public void testBinaryMetadataDir() throws Exception {
        IgniteEx grid = startGrid();

        grid.binary().builder("TestType").setField("TestField", 1).build();

        stopGrid();

        File metaDir = U.resolveWorkDirectory(U.defaultWorkDirectory(),
                DataStorageConfiguration.DFLT_BINARY_METADATA_PATH, false);

        assertTrue(F.isEmpty(metaDir.list()));
    }
}
