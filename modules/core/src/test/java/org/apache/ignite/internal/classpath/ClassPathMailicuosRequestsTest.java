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

package org.apache.ignite.internal.classpath;

import java.nio.file.Path;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class ClassPathMailicuosRequestsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** */
    @Test
    public void testMaliciousFilename() {
        String[] hackNames = new String[]{
            "/",
            "../../optional/ignite-cdc/myjar.jar",
            "./file.txt",
            "../file.txt",
            "/file.txt",
            "~/file.txt"
        };

        for (String hackName : hackNames) {
            assertThrows(
                null,
                () -> cpProc().startCreation("mycp", new String[]{hackName}, new long[]{42}),
                IllegalArgumentException.class,
                "simple filename expected"
            );
        }
    }

    /** */
    @Test
    public void testMaliciousClassPathName() {
        String[] hackNames = new String[]{
            "/",
            "../../optional/ignite-cdc",
            "./files",
            "../files",
            "/files",
            "~/files"
        };

        for (String hackName : hackNames) {
            assertThrows(
                null,
                () -> cpProc().startCreation(hackName, new String[]{"file.txt"}, new long[]{42}),
                IllegalArgumentException.class,
                "Classpath name must satisfy the following name pattern: a-zA-Z0-9_"
            );
        }
    }

    /** */
    @Test
    public void testUnknownFilename() throws IgniteCheckedException {
        UUID icpId0 = cpProc().startCreation("mycp", new String[]{"file.txt"}, new long[]{42});

        assertThrows(
            null,
            () -> {
                cpProc().copyClassPathFileLocally(icpId0, Path.of("other.txt"));
                return null;
            },
            IgniteException.class,
            "Unknown lib"
        );

        UUID icpId1 = cpProc().startCreation("mycp", new String[]{"file.txt"}, new long[]{42});

        assertThrows(
            null,
            () -> {
                cpProc().writeFilePartFromClient(icpId1, "other.txt", 0, new byte[1]);
                return null;
            },
            IgniteException.class,
            "Unknown lib"
        );
    }

    /** */
    @Test
    public void testWrongOffset() throws IgniteCheckedException {
        UUID icpId = cpProc().startCreation("mycp", new String[]{"file.txt"}, new long[]{42});

        assertThrows(
            null,
            () -> {
                cpProc().writeFilePartFromClient(icpId, "file.txt", U.GB, new byte[] {0});
                return null;
            },
            IgniteException.class,
            "Unexpected file offset [icp=mycp, file=file.txt]"
        );
    }


    /** */
    private ClassPathProcessor cpProc() {
        return grid(0).context().classPath();
    }
}
