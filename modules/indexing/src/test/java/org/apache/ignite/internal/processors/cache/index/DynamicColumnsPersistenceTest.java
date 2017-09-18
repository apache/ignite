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

package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryField;

/**
 * Created by apaschenko on 18.09.17.
 */
public class DynamicColumnsPersistenceTest extends DynamicColumnsAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        // Client configuration has SQL cache added to it, therefore it's used a basis.
        IgniteConfiguration cfg = clientConfiguration(0).setClientMode(false);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
        );

        return cfg;
    }

    /**
     * Test that dynamically added column is persisted along with the rest of cache configuration.
     * @throws Exception
     */
    public void testDynamicColumnsMetadataPersistence() throws Exception {
        IgniteEx node = startGrid(getConfiguration());

        node.active(true);

        node.cache("idx");

        run(node, "ALTER TABLE \"idx\".Person ADD COLUMN age int");

        doSleep(500);

        QueryField c = c("AGE", Integer.class.getName());

        checkNodeState(node, "idx", "PERSON", c);

        stopGrid(0);

        node = startGrid(getConfiguration());

        node.active(true);

        checkNodeState(node, "idx", "PERSON", c);
    }
}
