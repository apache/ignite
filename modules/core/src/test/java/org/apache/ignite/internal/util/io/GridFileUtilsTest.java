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

package org.apache.ignite.internal.util.io;

import java.nio.file.Path;
import org.apache.ignite.IgniteException;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.internal.util.io.GridFileUtils.ensureHardLinkAvailable;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link GridFileUtils} tests.
 */
@SuppressWarnings("ThrowableNotThrown")
public class GridFileUtilsTest {
    /** @throws Exception If failed. */
    @Test
    public void testEnsureHardLinkAvailable() throws Exception {
        Path path1 = mock(Path.class, Mockito.RETURNS_DEEP_STUBS);

        when(path1.getFileSystem().provider().getFileStore(any()).name()).thenReturn("disk1");

        Path path2 = mock(Path.class, Mockito.RETURNS_DEEP_STUBS);

        when(path2.getFileSystem().provider().getFileStore(any()).name()).thenReturn("disk2");

        ensureHardLinkAvailable(path1, path1);

        assertThrows(null, () -> ensureHardLinkAvailable(path1, path2), IgniteException.class,
            "Paths are not stored at the same device or partition.");
    }
}
