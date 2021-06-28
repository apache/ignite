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
import java.nio.file.Paths;
import org.apache.ignite.internal.storage.AbstractStorageTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Storage test implementation for {@link RocksDbStorage}.
 */
public class RocksDbStorageTest extends AbstractStorageTest {
    private Path path;

    @BeforeEach
    public void setUp() throws Exception {
        path = Paths.get("rocksdb_test");

        IgniteUtils.delete(path);

        storage = new RocksDbStorage(path, ByteBuffer::compareTo);
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            if (storage != null)
                ((AutoCloseable)storage).close();
        }
        finally {
            IgniteUtils.delete(path);
        }
    }
}
