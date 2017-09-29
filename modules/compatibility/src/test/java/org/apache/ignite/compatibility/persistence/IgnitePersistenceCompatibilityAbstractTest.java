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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Super class for all persistence compatibility tests.
 */
public abstract class IgnitePersistenceCompatibilityAbstractTest extends IgniteCompatibilityAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (!isDefaultDBWorkDirectoryEmpty())
            deleteDefaultDBWorkDirectory();

        assert isDefaultDBWorkDirectoryEmpty() : "DB work directory is not empty.";
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        assert deleteDefaultDBWorkDirectory() : "Couldn't delete DB work directory.";
    }

    /**
     * Gets a path to the default DB working directory.
     *
     * @return Path to the default DB working directory.
     * @throws IgniteCheckedException In case of an error.
     * @see #deleteDefaultDBWorkDirectory()
     * @see #isDefaultDBWorkDirectoryEmpty()
     */
    protected Path getDefaultDbWorkPath() throws IgniteCheckedException {
        return Paths.get(U.defaultWorkDirectory() + File.separator + DFLT_STORE_DIR);
    }

    /**
     * Deletes the default DB working directory with all sub-directories and files.
     *
     * @return {@code true} if and only if the file or directory is successfully deleted, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultDbWorkPath()
     * @see #deleteDefaultDBWorkDirectory()
     */
    protected boolean deleteDefaultDBWorkDirectory() throws IgniteCheckedException {
        Path dir = getDefaultDbWorkPath();

        return Files.notExists(dir) || U.delete(dir.toFile());
    }

    /**
     * Checks if the default DB working directory is empty.
     *
     * @return {@code true} if the default DB working directory is empty or doesn't exist, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultDbWorkPath()
     * @see #deleteDefaultDBWorkDirectory()
     */
    @SuppressWarnings("ConstantConditions")
    protected boolean isDefaultDBWorkDirectoryEmpty() throws IgniteCheckedException {
        File dir = getDefaultDbWorkPath().toFile();

        return !dir.exists() || (dir.isDirectory() && dir.list().length == 0);
    }
}
