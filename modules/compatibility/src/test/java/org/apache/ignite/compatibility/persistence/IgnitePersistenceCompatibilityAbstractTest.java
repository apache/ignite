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
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarFile;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
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

        boolean isEmpty = isDefaultDBWorkDirectoryEmpty();

        assert isEmpty : "DB work directory is not empty: " + getDefaultDbWorkPath();

        if (!isDefaultBinaryMetaDirectoryEmpty())
            deleteDefaultBinaryMetaDirectory();

        isEmpty = deleteDefaultBinaryMetaDirectory();

        assert isEmpty : "Binary meta directory is not empty: " + getDefaultBinaryMetaPath();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        boolean deleted = deleteDefaultDBWorkDirectory();

        assert deleted : "Couldn't delete DB work directory.";

        deleted = deleteDefaultBinaryMetaDirectory();

        assert deleted : "Couldn't delete binary meta directory.";
    }

    /**
     * Gets a path to the default DB working directory.
     *
     * @return Path to the default DB working directory.
     * @throws IgniteCheckedException In case of an error.
     * @see #deleteDefaultDBWorkDirectory()
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
        return deleteDirectory(getDefaultDbWorkPath());
    }

    /**
     * Checks if the default DB working directory is empty.
     *
     * @return {@code true} if the default DB working directory is empty or doesn't exist, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultBinaryMetaPath()
     * @see #deleteDefaultDBWorkDirectory()
     */
    @SuppressWarnings("ConstantConditions")
    protected boolean isDefaultDBWorkDirectoryEmpty() throws IgniteCheckedException {
        return isDirectoryEmpty(getDefaultDbWorkPath().toFile());
    }

    /**
     * Gets a path to the default binary meta directory.
     *
     * @return Path to the default binary meta directory.
     * @throws IgniteCheckedException In case of an error.
     */
    protected Path getDefaultBinaryMetaPath() throws IgniteCheckedException {
        return Paths.get(U.defaultWorkDirectory() + File.separator + StandaloneGridKernalContext.BINARY_META_FOLDER);
    }

    /**
     * Deletes the default binary meta directory with all sub-directories and files.
     *
     * @return {@code true} if and only if the file or directory is successfully deleted, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultBinaryMetaPath()
     * @see #isDefaultBinaryMetaDirectoryEmpty()
     */
    protected boolean deleteDefaultBinaryMetaDirectory() throws IgniteCheckedException {
        return deleteDirectory(getDefaultBinaryMetaPath());
    }

    /**
     * Checks if the default binary meta directory is empty.
     *
     * @return {@code true} if the default binary meta directory is empty or doesn't exist, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultBinaryMetaPath()
     * @see #deleteDefaultBinaryMetaDirectory()
     */
    @SuppressWarnings("ConstantConditions")
    protected boolean isDefaultBinaryMetaDirectoryEmpty() throws IgniteCheckedException {
        return isDirectoryEmpty(getDefaultBinaryMetaPath().toFile());
    }

    /**
     * Deletes given directory if exists.
     *
     * @param dir Directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     */
    protected boolean deleteDirectory(Path dir) {
        return Files.notExists(dir) || delete(dir);
    }

    /**
     * Checks if the given directory is empty.
     *
     * @param dir Directory to check.
     * @return {@code true} if the given directory is empty or doesn't exist, otherwise {@code false}.
     */
    @SuppressWarnings("ConstantConditions")
    protected boolean isDirectoryEmpty(File dir) {
        return !dir.exists() || (dir.isDirectory() && dir.list().length == 0);
    }

    /**
     * Deletes file or directory with all sub-directories and files.
     *
     * @param path File or directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     * {@code false} otherwise
     */
    public static boolean delete(Path path) {
        if (Files.isDirectory(path)) {
            try {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                    for (Path innerPath : stream) {
                        boolean res = delete(innerPath);

                        if (!res)
                            return false;
                    }
                }
            }
            catch (IOException e) {
                return false;
            }
        }

        if (path.endsWith("jar")) {
            try {
                // Why do we do this?
                new JarFile(path.toString(), false).close();
            }
            catch (IOException ignore) {
                // Ignore it here...
            }
        }

        try {
            Files.delete(path);

            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

}
