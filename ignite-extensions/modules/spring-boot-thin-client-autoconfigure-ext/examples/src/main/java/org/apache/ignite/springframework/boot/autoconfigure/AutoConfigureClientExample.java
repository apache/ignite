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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;

/** Example of Ignite client auto configuration. */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class AutoConfigureClientExample {
    /**
     * Main method of the application.
     * @param args Arguments.
     */
    public static void main(String[] args) {
        //Starting Ignite server node outside of Spring Boot application so client can connect to it.
        Ignite serverNode = Ignition.start(new IgniteConfiguration());

        //Creating caches.
        serverNode.createCache("my-cache1");
        serverNode.createCache("my-cache2");

        SpringApplication.run(AutoConfigureClientExample.class);
    }

    /**
     * @return Configurer for the Ignite client.
     */
    @Bean
    IgniteClientConfigurer configurer() {
        //Setting some property.
        //Other will come from `application.yml`
        return cfg -> cfg.setSendBufferSize(64 * 1024);
    }

    /**
     * Service using autoconfigured Ignite.
     * @return Runner.
     */
    @Bean
    CommandLineRunner runner() {
        return new CommandLineRunner() {
            /** IgniteClient instance. */
            @Autowired
            private IgniteClient client;

            /** Method will be executed on application startup. */
            @Override public void run(String... args) throws Exception {
                System.out.println("ServiceWithIgniteClient.run");
                System.out.println("Cache names existing in cluster: " + client.cacheNames());

                ClientCache<Integer, Integer> cache = client.cache("my-cache1");

                System.out.println("Putting data to the my-cache1...");

                cache.put(1, 1);
                cache.put(2, 2);
                cache.put(3, 3);

                System.out.println("Done putting data to the my-cache1...");
            }
        };
    }
}
