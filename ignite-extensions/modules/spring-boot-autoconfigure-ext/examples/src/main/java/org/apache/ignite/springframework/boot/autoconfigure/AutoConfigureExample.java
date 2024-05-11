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

package org.apache.ignite.springframework.boot.autoconfigure;

import java.util.Date;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;

/** Example of Ignite auto configurer. */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class AutoConfigureExample {
    /**
     * Main method of the application.
     * @param args Arguments.
     */
    public static void main(String[] args) {
        SpringApplication.run(AutoConfigureExample.class, args);
    }

    /**
     * Providing configurer for the Ignite.
     * @return Ignite Configurer.
     */
    @Bean
    public IgniteConfigurer configurer() {
        return cfg -> {
            //Setting consistent id.
            //See `application.yml` for the additional properties.
            cfg.setConsistentId("consistent-id");
            cfg.setCommunicationSpi(new TcpCommunicationSpi());
        };
    }

    /**
     * Service using autoconfigured Ignite.
     * @return Runner.
     */
    @Bean
    public CommandLineRunner runner() {
        return new CommandLineRunner() {
            /** Ignite instance. */
            @Autowired
            private Ignite ignite;

            /** Method will be executed on application startup. */
            @Override public void run(String... args) throws Exception {
                System.out.println("ServiceWithIgnite.run:");
                //This property comes from configurer. See AutoConfigureExample.
                System.out.println("    IgniteConsistentId: " + ignite.configuration().getConsistentId());

                //Other properties are set via application-node.yml.
                System.out.println("    IgniteInstanceName: " + ignite.configuration().getIgniteInstanceName());
                System.out.println("    CommunicationSpi.localPort: " +
                    ((TcpCommunicationSpi)ignite.configuration().getCommunicationSpi()).getLocalPort());
                System.out.println("    DefaultDataRegion initial size: " +
                    ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().getInitialSize());

                DataRegionConfiguration drc =
                    ignite.configuration().getDataStorageConfiguration().getDataRegionConfigurations()[0];

                System.out.println("    " + drc.getName() + " initial size: " + drc.getInitialSize());
                System.out.println("    Cache in cluster:");

                for (String cacheName : ignite.cacheNames())
                    System.out.println("        " + cacheName);

                cacheAPI();
                sqlAPI();
            }

            /** Example of the SQL API usage. */
            private void sqlAPI() {
                //This cache configured in `application.yml`.
                IgniteCache<Long, Object> accounts = ignite.cache("accounts");

                //SQL table configured via QueryEntity in `application.yml`
                String qry = "INSERT INTO ACCOUNTS(ID, AMOUNT, UPDATEDATE) VALUES(?, ?, ?)";

                accounts.query(new SqlFieldsQuery(qry).setArgs(1, 250.05, new Date())).getAll();
                accounts.query(new SqlFieldsQuery(qry).setArgs(2, 255.05, new Date())).getAll();
                accounts.query(new SqlFieldsQuery(qry).setArgs(3, .05, new Date())).getAll();

                qry = "SELECT * FROM ACCOUNTS";

                List<List<?>> res = accounts.query(new SqlFieldsQuery(qry)).getAll();

                for (List<?> row : res)
                    System.out.println("(" + row.get(0) + ", " + row.get(1) + ", " + row.get(2) + ")");
            }

            /** Example of the Cache API usage. */
            private void cacheAPI() {
                //This cache configured in `application.yml`.
                IgniteCache<Integer, Integer> cache = ignite.cache("my-cache2");

                System.out.println("Putting data to the my-cache1...");

                cache.put(1, 1);
                cache.put(2, 2);
                cache.put(3, 3);

                System.out.println("Done putting data to the my-cache1...");
            }
        };
    }
}
