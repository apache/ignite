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
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.compatibility.testframework.util.CompatibilityTestsUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Super class for all persistence compatibility tests.
 */
public abstract class IgnitePersistenceCompatibilityAbstractTest extends IgniteCompatibilityAbstractTest {
    /** Persistence directories. */
    private static final List<String> PERSISTENCE_DIRS = Arrays.asList(DFLT_STORE_DIR, "binary_meta", "cp", "marshaller");

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (String dirName : PERSISTENCE_DIRS) {
            File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), dirName, true);

            if (!CompatibilityTestsUtils.isDirectoryEmpty(dir)) {
                throw new IllegalStateException("Directory is not empty and can't be cleaned: " +
                    "[" + dir.getAbsolutePath() + "]. It may be locked by another system process.");
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        for (String dir : PERSISTENCE_DIRS)
            U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), dir, false));
    }
}
