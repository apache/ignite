/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.springdata;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.springdata20.repository.IgniteRepository;
import org.apache.ignite.springdata20.repository.config.EnableIgniteRepositories;
import org.apache.ignite.springdata20.repository.support.IgniteRepositoryFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Every {@link IgniteRepository} is bound to a specific Apache Ignite that it communicates to in order to mutate and
 * read data via Spring Data API. To pass an instance of Apache Ignite cache to an {@link IgniteRepository} it's
 * required to initialize {@link IgniteRepositoryFactoryBean} with on of the following:
 * <ul>
 * <li>{@link Ignite} instance bean named "igniteInstance"</li>
 * <li>{@link IgniteConfiguration} bean named "igniteCfg"</li>
 * <li>A path to Ignite's Spring XML configuration named "igniteSpringCfgPath"</li>
 * <ul/>
 * In this example the first approach is utilized.
 */
@Configuration
@EnableIgniteRepositories
public class SpringAppCfg {
    /**
     * Creating Apache Ignite instance bean. A bean will be passed to {@link IgniteRepositoryFactoryBean} to initialize
     * all Ignite based Spring Data repositories and connect to a cluster.
     */
    @Bean
    public Ignite igniteInstance() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Setting some custom name for the node.
        cfg.setIgniteInstanceName("springDataNode");

        // Enabling peer-class loading feature.
        cfg.setPeerClassLoadingEnabled(true);

        // Defining and creating a new cache to be used by Ignite Spring Data repository.
        CacheConfiguration ccfg = new CacheConfiguration("PersonCache");

        // Setting SQL schema for the cache.
        ccfg.setIndexedTypes(Long.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return Ignition.start(cfg);
    }
}
