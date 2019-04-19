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

package org.apache.ignite.transactions.spring;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

public class GridSpringTransactionManagerSpringBeanSelfTest extends GridSpringTransactionManagerAbstractTest {

    /** */
    private Ignite ignite;

    /** */
    private GridSpringTransactionService service;

    @Override public IgniteCache<Integer, String> cache() {
        return ignite.cache(CACHE_NAME);
    }

    @Override public GridSpringTransactionService service() {
        return service;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ApplicationContext appCtx = new GenericXmlApplicationContext("config/spring-transactions-ignite-spring-bean.xml");

        // To produce multiple calls of ApplicationListener::onApplicationEvent
        GenericXmlApplicationContext child = new GenericXmlApplicationContext();
        child.setParent(appCtx);
        child.refresh();

        ignite = (Ignite)appCtx.getBean("mySpringBean");
        service = (GridSpringTransactionService)appCtx.getBean("gridSpringTransactionService");
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }
}
