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

package org.apache.ignite.internal.storage.rocksdb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.internal.storage.AbstractStorageTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Storage test implementation for {@link RocksDbStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbStorageTest extends AbstractStorageTest {
    /** */
    @BeforeEach
    public void setUp(@WorkDirectory Path workDir) {
        storage = new RocksDbStorage(workDir, ByteBuffer::compareTo);
    }

    /** */
    @AfterEach
    public void tearDown() throws Exception {
        if (storage != null)
            ((AutoCloseable)storage).close();
    }
}
