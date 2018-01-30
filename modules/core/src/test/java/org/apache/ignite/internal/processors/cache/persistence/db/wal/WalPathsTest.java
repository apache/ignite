/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class WalPathsTest extends GridCommonAbstractTest {
    /** WalPath and WalArchivePath. */
    private File file;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        U.delete(file);
    }

    /**
     * @param relativePath {@code True} - if wal archive path should be relative, {@code false} - for absolute path.
     * @return Ignite configuration with the same path to wal store and wal archive.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getConfig(boolean relativePath) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        file = new File(U.defaultWorkDirectory() + File.separatorChar + getClass().getSimpleName());

        dsCfg.setWalPath(file.getAbsolutePath());
        dsCfg.setWalArchivePath(relativePath ? getClass().getSimpleName() : file.getAbsolutePath());

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * Tests equal paths to the same directory.
     *
     * @throws Exception If failed.
     */
    public void testWalStoreAndArchivePathsEquality() throws Exception {
        IgniteConfiguration cfg = getConfig(false);

        startGrid(cfg);
    }

    /**
     * Tests absolute and relative paths to the same directory.
     *
     * @throws Exception If failed.
     */
    public void testWalStoreAndArchiveAbsolutAndRelativePathsEquality() throws Exception {
        final IgniteConfiguration cfg = getConfig(true);

        startGrid(cfg);
    }
}
