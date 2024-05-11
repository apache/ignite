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
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories;
import org.apache.ignite.springdata.repository.config.IgniteRepositoryConfigurationExtension;
import org.apache.ignite.springdata.repository.support.IgniteRepositoryFactoryBean;
import org.apache.ignite.springframework.boot.autoconfigure.data.IgniteRepositoriesAutoConfigurationRegistrar;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;



/**
 * {@link EnableAutoConfiguration Auto-configuration} for Ignite Spring Data's Repositories.
 * <p>
 * Activates when there is a bean of type {@link org.apache.ignite.Ignite} configured in the
 * context, the Ignite Spring Data {@link IgniteRepository} type is on the classpath,
 * and there is no other, existing {@link IgniteRepository} configured.
 * <p>
 * Once in effect, the auto-configuration is the equivalent of enabling Ignite repositories
 * using the {@link EnableIgniteRepositories @EnableIgniteRepositories} annotation.
 * <p>
 * This configuration class will activate <em>after</em> the Ignite node auto-configuration.
 */
@Configuration
@ConditionalOnBean(Ignite.class)
@ConditionalOnClass(IgniteRepository.class)
@ConditionalOnMissingBean({IgniteRepositoryFactoryBean.class, IgniteRepositoryConfigurationExtension.class})
@Import(IgniteRepositoriesAutoConfigurationRegistrar.class)
@AutoConfigureAfter({IgniteAutoConfiguration.class})
public class IgniteRepositoryAutoConfiguration {

}
