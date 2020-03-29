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

package org.apache.ignite.springdata.repository.support;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

/**
 * Apache Ignite repository factory bean.
 *
 * The repository requires to define one of the parameters below in your Spring application configuration in order
 * to get an access to Apache Ignite cluster:
 * <ul>
 * <li>{@link Ignite} instance bean named "igniteInstance"</li>
 * <li>{@link IgniteConfiguration} bean named "igniteCfg"</li>
 * <li>A path to Ignite's Spring XML configuration named "igniteSpringCfgPath"</li>
 * <ul/>
 *
 * @param <T> Repository type, {@link IgniteRepository}
 * @param <S> Domain object class.
 * @param <ID> Domain object key, super expects {@link Serializable}.
 */
public class IgniteRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
    extends RepositoryFactoryBeanSupport<T, S, ID> implements ApplicationContextAware {
    /** Application context. */
    private ApplicationContext ctx;

    /**
     * @param repositoryInterface Repository interface.
     */
    protected IgniteRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext context) throws BeansException {
        this.ctx = context;
    }

    /** {@inheritDoc} */
    @Override protected RepositoryFactorySupport createRepositoryFactory() {
        try {
            Ignite ignite = (Ignite)ctx.getBean("igniteInstance");

            return new IgniteRepositoryFactory(ignite);
        }
        catch (BeansException ex) {
            try {
                IgniteConfiguration cfg = (IgniteConfiguration)ctx.getBean("igniteCfg");

                return new IgniteRepositoryFactory(cfg);
            }
            catch (BeansException ex2) {
                try {
                    String path = (String)ctx.getBean("igniteSpringCfgPath");

                    return new IgniteRepositoryFactory(path);
                }
                catch (BeansException ex3) {
                    throw new IgniteException("Failed to initialize Ignite repository factory. Ignite instance or" +
                        " IgniteConfiguration or a path to Ignite's spring XML configuration must be defined in the" +
                        " application configuration");
                }
            }
        }
    }
}

