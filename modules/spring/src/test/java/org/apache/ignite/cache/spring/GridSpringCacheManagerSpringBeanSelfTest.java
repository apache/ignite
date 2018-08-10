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

package org.apache.ignite.cache.spring;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Spring cache test.
 */
public class GridSpringCacheManagerSpringBeanSelfTest extends GridSpringCacheManagerAbstractTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        BeanFactory factory = new ClassPathXmlApplicationContext("org/apache/ignite/cache/spring/spring-caching-ignite-spring-bean.xml");

        svc = (GridSpringCacheTestService)factory.getBean("testService");
        dynamicSvc = (GridSpringDynamicCacheTestService)factory.getBean("dynamicTestService");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }
}
