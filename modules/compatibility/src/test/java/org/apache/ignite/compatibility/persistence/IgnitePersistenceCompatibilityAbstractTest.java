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

package org.apache.ignite.compatibility.persistence;

import java.io.File;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.compatibility.testframework.util.CompatibilityTestsUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;

/**
 * Super class for all persistence compatibility tests.
 */
public abstract class IgnitePersistenceCompatibilityAbstractTest extends IgniteCompatibilityAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        createAndCheck(sharedFileTree().db());
        createAndCheck(sharedFileTree().binaryMetaRoot());
        createAndCheck(sharedFileTree().marshaller());
        createAndCheck(U.resolveWorkDirectory(U.defaultWorkDirectory(), SharedFsCheckpointSpi.DFLT_ROOT, true));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        U.delete(sharedFileTree().db());
        U.delete(sharedFileTree().binaryMetaRoot());
        U.delete(sharedFileTree().marshaller());
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), SharedFsCheckpointSpi.DFLT_ROOT, false));
    }

    /** @param dir Directory to create and check. */
    private void createAndCheck(File dir) {
        U.delete(dir);

        assertTrue(dir.mkdirs());

        if (!CompatibilityTestsUtils.isDirectoryEmpty(dir)) {
            throw new IllegalStateException("Directory is not empty and can't be cleaned: " +
                "[" + dir.getAbsolutePath() + "]. It may be locked by another system process.");
        }
    }
}
