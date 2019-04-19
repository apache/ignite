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

package org.apache.ignite.internal.jdbc2;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;

/**
 * JDBC test of INSERT statement w/binary marshaller - no nodes know about classes.
 */
public class JdbcBinaryMarshallerInsertStatementSelfTest extends JdbcInsertStatementSelfTest {
    /** {@inheritDoc} */
    @Override protected String getCfgUrl() {
        return BASE_URL_BIN;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setMarshaller(new BinaryMarshaller());
    }

    /** {@inheritDoc} */
    @Override CacheConfiguration cacheConfig() {
        return binaryCacheConfig();
    }
}
