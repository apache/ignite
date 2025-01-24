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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.nio.file.Paths;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Class contains pathes to Ignite folder.
 * Note, that base path can be different for each usage:
 * <ul>
 *     <li>Ignite node.</li>
 *     <li>Snapshot files.</li>
 *     <li>Cache dump files</li>
 *     <li>CDC</li>
 * </ul>
 */
public class IgniteDirectories {
    /**
     * Path to the folder containing binary metadata.
     * Example: TODO add example.
     */
    private final File binaryMetaRoot;
    /**
     * Path to the folder containing binary metadata.
     * Example: TODO add example.
     */
    private final @Nullable File binaryMeta;

    /**
     * Path to the folder containing marshaller files.
     * Example: TODO add example.
     */
    private final File marshaller;

    public IgniteDirectories(String root) {
        A.notNullOrEmpty(root, "Root directory");

        marshaller = new File(root, DataStorageConfiguration.DFLT_MARSHALLER_PATH);
        binaryMetaRoot = Paths.get(root, DataStorageConfiguration.DFLT_BINARY_METADATA_PATH).toFile();
        binaryMeta = null;
    }

    /**
     * Root directory can be Ignite work directory, see {@link U#workDirectory(String, String)} and other methods.
     *
     * @param root Root directory.
     * @param folderName Name of the folder for current node.
     *                   Usually, it a {@link IgniteConfiguration#getConsistentId()} masked to be correct file name.
     *
     * @see IgniteConfiguration#getWorkDirectory()
     * @see IgniteConfiguration#setWorkDirectory(String)
     * @see U#workDirectory(String, String)
     * @see U#resolveWorkDirectory(String, String, boolean, boolean)
     * @see U#IGNITE_WORK_DIR
     */
    public IgniteDirectories(String root, String folderName) {
        A.notNullOrEmpty(root, "Root directory");
        A.notNullOrEmpty(folderName, "Node directory");

        marshaller = new File(root, DataStorageConfiguration.DFLT_MARSHALLER_PATH);
        binaryMetaRoot = Paths.get(root, DataStorageConfiguration.DFLT_BINARY_METADATA_PATH).toFile();
        binaryMeta = folderName == null ? null : Paths.get(binaryMetaRoot.getAbsolutePath(), folderName).toFile();
    }

    /**
     * @return Path to binary metadata directory. TODO: add example.
     */
    public File binaryMeta() {
        return binaryMeta;
    }

    /**
     * @return Path to common binary metadata directory. Note, directory can contains data from several nodes.
     * Each node will create own directory inside this root.
     * TODO: add example.
     */
    public File binaryMetaRoot() {
        return binaryMetaRoot;
    }

    /**
     * @return Path to marshaller directory. TODO add example.
     */
    public File marshaller() {
        return marshaller;
    }

    /**
     * Creates {@link #binaryMeta()} directory.
     * @return Created directory.
     * @see #binaryMeta()
     */
    public File mkdirBinaryMeta() {
        if (!U.mkdirs(binaryMeta))
            throw new IgniteException("Could not create directory for binary metadata: " + binaryMeta);

        return binaryMeta;
    }

    /**
     * Creates {@link #marshaller()} directory.
     * @return Created directory.
     * @see #marshaller()
     */
    public File mkdirMarshaller() {
        if (!U.mkdirs(marshaller))
            throw new IgniteException("Could not create directory for marshaller mappings: " + marshaller);

        return marshaller;
    }
}
