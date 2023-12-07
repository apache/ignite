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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Ckeck that WAL manager closes File IO interfaces.
 */
@RunWith(Parameterized.class)
public class WalFilesCloseTest extends GridCommonAbstractTest {
    /** Opened File IO interfaces. */
    private final List<FileIO> opened = new CopyOnWriteArrayList<>();

    /** */
    @Parameterized.Parameter
    public WALMode mode;

    /** */
    @Parameterized.Parameters(name = "mode={0}")
    public static Object[] parameters() {
        return new Object[] {WALMode.FSYNC, WALMode.LOG_ONLY, WALMode.BACKGROUND};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(mode)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
            ));

        cfg.setPluginProviders(new TestWalManagerProvider());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testStartStopServer() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.close();

        assertTrue(opened.isEmpty());
    }

    /** Test class to track opened file IO interfaces. */
    private class TestWalManagerProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "testPlugin";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (!IgniteWriteAheadLogManager.class.equals(cls))
                return null;

            FileWriteAheadLogManager wal = new FileWriteAheadLogManager(((IgniteEx)ctx.grid()).context());

            FileIOFactory delegate = GridTestUtils.getFieldValue(wal, "ioFactory");

            wal.setFileIOFactory(new FileIOFactory() {
                @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                    FileIODecorator fileIo = new FileIODecorator(delegate.create(file, modes)) {
                        @Override public void close() throws IOException {
                            super.close();

                            opened.remove(this);
                        }
                    };

                    opened.add(fileIo);

                    return fileIo;
                }
            });

            return (T)wal;
        }
    }
}
