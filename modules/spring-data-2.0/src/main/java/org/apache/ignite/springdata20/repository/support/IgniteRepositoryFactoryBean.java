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

package org.apache.ignite.springdata20.repository.support;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
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
