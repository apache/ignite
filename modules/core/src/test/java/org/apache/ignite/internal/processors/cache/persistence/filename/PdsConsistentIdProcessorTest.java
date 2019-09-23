/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.util.regex.Pattern;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class PdsConsistentIdProcessorTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /**
     * Issue - https://ggsystems.atlassian.net/browse/GG-22488
     * Test plan - https://ggsystems.atlassian.net/browse/GG-24044
     *
     * @throws Exception If exception.
     */
    @Test
    public void testLogMessageOnNodeStart() throws Exception {
        startNode(getConfiguration("node_without_id").setConsistentId(null));

        startNode(getConfiguration("node_with_id").setConsistentId("id"));
    }

    /**
     * @param cfg Ignite configuration.
     * @throws Exception If exception.
     */
    private void startNode(IgniteConfiguration cfg) throws Exception {
        LogListener lsn;

        if (cfg.getConsistentId() != null) {
            String msg = "Consistent ID used for local node is [" + cfg.getConsistentId() + "] " +
                "according to persistence data storage folders";

            lsn = LogListener.matches(msg).build();
        }
        else {
            Pattern pattern = Pattern.compile("Consistent ID used for local node is" +
                " \\[[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}]");

            lsn = LogListener.matches(pattern).build();
        }

        ListeningTestLogger logger = new ListeningTestLogger();

        logger.registerListener(lsn);

        cfg.setGridLogger(logger);

        startGrid(cfg);

        Assert.assertTrue("Missed message on node startup", lsn.check());
    }
}