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

package org.apache.ignite.springdata20.repository.support;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata20.repository.IgniteRepository;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

/**
 * Apache Ignite repository factory bean.
 * <p>
 * The {@link org.apache.ignite.springdata20.repository.config.RepositoryConfig} requires to define one of the
 * parameters below in your Spring application configuration in order to get an access to Apache Ignite cluster:
 * <ul>
 * <li>{@link Ignite} instance bean named "igniteInstance" by default</li>
 * <li>{@link IgniteConfiguration} bean named "igniteCfg" by default</li>
 * <li>A path to Ignite's Spring XML configuration named "igniteSpringCfgPath" by default</li>
 * <ul/>
 *
 * @param <T> Repository type, {@link IgniteRepository}
 * @param <V> Domain object class.
 * @param <K> Domain object key, super expects {@link Serializable}.
 */
public class IgniteRepositoryFactoryBean<T extends Repository<V, K>, V, K extends Serializable>
    extends RepositoryFactoryBeanSupport<T, V, K> implements ApplicationContextAware {
    /** */
    private ApplicationContext ctx;

    /**
     * @param repoInterface Repository interface.
     */
    protected IgniteRepositoryFactoryBean(Class<? extends T> repoInterface) {
        super(repoInterface);
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override protected RepositoryFactorySupport createRepositoryFactory() {
        return new IgniteRepositoryFactory(ctx);
    }
}
