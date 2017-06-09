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

package org.apache.ignite.yakov;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class YakovRunner {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration()
                .setTypeName(ES_BS_CONTRACT_HISTORY_KEY.class.getName())
        ));

        Ignite ignite = Ignition.start(new IgniteConfiguration().setBinaryConfiguration(binCfg).setIgniteInstanceName("node1").setLocalHost("127.0.0.1"));
        Ignite ignite2 = Ignition.start(new IgniteConfiguration().setBinaryConfiguration(binCfg).setIgniteInstanceName("node2").setLocalHost("127.0.0.1").setClientMode(true));

        CacheConfiguration ccfg = new CacheConfiguration("CACHE")
            .setIndexedTypes(ES_BS_CONTRACT_HISTORY_KEY.class,
            ES_BS_CONTRACT_HISTORY.class);
        IgniteCache cache = ignite.getOrCreateCache(ccfg);
        cache.put(new ES_BS_CONTRACT_HISTORY_KEY(1, 2), new ES_BS_CONTRACT_HISTORY("3", "4", "5"));
        ignite2.cache("CACHE").query(new SqlFieldsQuery("INSERT INTO ES_BS_CONTRACT_HISTORY (CO_ID, CH_SEQNO, CH_STATUS, CH_PENDING, TIMESTAMP) VALUES (?, ?, ?, ?, ?)").setArgs(11, 22, "33", "44", "55")).getAll();
        System.out.println("ALL IS OK");
    }
    public static class ES_BS_CONTRACT_HISTORY_KEY implements Serializable {
        private static final long serialVersionUID = 1L;
        @AffinityKeyMapped
        @QuerySqlField(index = true)
        public int CO_ID;
        @QuerySqlField
        public int CH_SEQNO;
        public ES_BS_CONTRACT_HISTORY_KEY(int CO_ID, int CH_SEQNO) {
            this.CO_ID = CO_ID;
            this.CH_SEQNO = CH_SEQNO;
        }
        @Override
        public int hashCode() {
            return super.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }
    public static class ES_BS_CONTRACT_HISTORY implements Serializable {
        private static final long serialVersionUID = 7088842328911005885L;
        @QuerySqlField
        public String CH_STATUS;
        @QuerySqlField
        public String CH_PENDING;
        @QuerySqlField
        public String TIMESTAMP;
        public ES_BS_CONTRACT_HISTORY(final String CH_STATUS, final String CH_PENDING, final String TIMESTAMP) {
            this.CH_STATUS = CH_STATUS;
            this.CH_PENDING = CH_PENDING;
            this.TIMESTAMP = TIMESTAMP;
        }
    }
}
