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

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.StandardBeanExpressionResolver;

import static org.apache.ignite.springdata.repository.support.IgniteRepositoryFactory.getRepositoryConfiguration;

/**
 * Represents factory for obtaining instances of {@link IgniteProxy} that provide client-independent connection to the
 * Ignite cluster.
 */
public class IgniteProxyFactory implements ApplicationContextAware, DisposableBean {
    /** Spring application expression resolver. */
    private final BeanExpressionResolver expressionResolver = new StandardBeanExpressionResolver();

    /** Repositories associated with Ignite proxy. */
    private final Map<Class<?>, IgniteProxy> igniteProxies = new ConcurrentHashMap<>();

    /** Spring application context. */
    private ApplicationContext ctx;

    /** Spring application bean expression context. */
    private BeanExpressionContext beanExpressionCtx;

    /**
     * @param repoInterface The repository interface class for which {@link IgniteProxy} will be created.
     * @return {@link IgniteProxy} instance.
     */
    public IgniteProxy igniteProxy(Class<?> repoInterface) {
        return igniteProxies.computeIfAbsent(repoInterface, k -> createIgniteProxy(repoInterface));
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.ctx = ctx;

        beanExpressionCtx = new BeanExpressionContext(
            new DefaultListableBeanFactory(ctx.getAutowireCapableBeanFactory()),
            null);
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws Exception {
        Set<IgniteProxy> proxies = new HashSet<>(igniteProxies.values());

        Exception destroyE = null;

        for (IgniteProxy proxy : proxies) {
            if (proxy instanceof AutoCloseable) {
                try {
                    ((AutoCloseable)proxy).close();
                }
                catch (Exception e) {
                    if (destroyE == null)
                        destroyE = e;
                    else
                        destroyE.addSuppressed(e);
                }
            }
        }

        if (destroyE != null)
            throw destroyE;
    }

    /**
     * Creates {@link IgniteProxy} to be used for providing access to the Ignite cluster for specified repository.
     *
     * @param repoInterface {@link Class} instance of the repository interface.
     * @return Instance of {@link IgniteProxy} associated with specified repository.
     *
     * @see RepositoryConfig
     */
    private IgniteProxy createIgniteProxy(Class<?> repoInterface) {
        RepositoryConfig repoCfg = getRepositoryConfiguration(repoInterface);

        return Stream.<BeanFinder>of(
            () -> ctx.getBean(evaluateExpression(repoCfg.igniteInstance())),
            () -> ctx.getBean(evaluateExpression(repoCfg.igniteCfg())),
            () -> ctx.getBean(evaluateExpression(repoCfg.igniteSpringCfgPath()), String.class),
            () -> ctx.getBean(Ignite.class),
            () -> ctx.getBean(IgniteClient.class),
            () -> ctx.getBean(IgniteConfiguration.class),
            () -> ctx.getBean(ClientConfiguration.class)
        ).map(BeanFinder::getBean)
            .filter(Objects::nonNull)
            .findFirst()
            .map(IgniteProxy::of)
            .orElseThrow(() -> {
                return new IllegalArgumentException("Invalid configuration for repository " +
                    repoInterface.getName() + ". No beans were found that provide connection configuration to the" +
                    " Ignite cluster. Check \"igniteInstance\", \"igniteCfg\", \"igniteSpringCfgPath\" parameters" +
                    " of " + RepositoryConfig.class.getName() + " repository annotation or provide Ignite, IgniteClient, " +
                    " ClientConfiguration or IgniteConfiguration bean to application context.");
            });
    }

    /**
     * Evaluates the SpEL expression.
     *
     * @param spelExpression SpEL expression
     * @return The result of evaluation of the SpEL expression.
     */
    private String evaluateExpression(String spelExpression) {
        return (String)expressionResolver.evaluate(spelExpression, beanExpressionCtx);
    }

    /**
     * Helper interface that wraps getBean method.
     */
    @FunctionalInterface
    private interface BeanFinder {
        /**
         * Get bean.
         * @return Bean or null if {@link BeansException} was thrown.
         */
        default Object getBean() {
            try {
                return get();
            }
            catch (BeansException ex) {
                return null;
            }
        }

        /**
         * Get bean.
         * @return Bean.
         * @throws BeansException If bean was not found.
         */
        Object get() throws BeansException;
    }
}
