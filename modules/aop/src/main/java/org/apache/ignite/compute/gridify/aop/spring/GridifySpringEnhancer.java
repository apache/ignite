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

package org.apache.ignite.compute.gridify.aop.spring;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;

/**
 * Spring AOP enhancer. Use it to grid-enable methods annotated with
 * {@link org.apache.ignite.compute.gridify.Gridify}, {@link org.apache.ignite.compute.gridify.GridifySetToValue} and {@link org.apache.ignite.compute.gridify.GridifySetToSet} annotations.
 * <p>
 * Note, that Spring AOP requires that all grid-enabled methods must
 * be {@code enhanced} because it is proxy-based. Other AOP implementations,
 * such as JBoss or AspectJ don't require special handling.
 * <p>
 * See {@link org.apache.ignite.compute.gridify.Gridify} documentation for more information about execution of
 * {@code gridified} methods.
 * @see org.apache.ignite.compute.gridify.Gridify
 * @see org.apache.ignite.compute.gridify.GridifySetToValue
 * @see org.apache.ignite.compute.gridify.GridifySetToSet
 */
public final class GridifySpringEnhancer {
    /** Spring aspect. */
    private static final GridifySpringAspect dfltAsp = new GridifySpringAspect();

    /** Spring aspect. */
    private static final GridifySetToSetSpringAspect setToSetAsp = new GridifySetToSetSpringAspect();

    /** Spring aspect. */
    private static final GridifySetToValueSpringAspect setToValAsp = new GridifySetToValueSpringAspect();

    /**
     * Enforces singleton.
     */
    private GridifySpringEnhancer() {
        // No-op.
    }

    /**
     * Enhances the object on load.
     *
     * @param <T> Type of the object to enhance.
     * @param obj Object to augment/enhance.
     * @return Enhanced object.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> T enhance(T obj) {
        ProxyFactory proxyFac = new ProxyFactory(obj);

        proxyFac.addAdvice(dfltAsp);
        proxyFac.addAdvice(setToValAsp);
        proxyFac.addAdvice(setToSetAsp);

        while (proxyFac.getAdvisors().length > 0)
            proxyFac.removeAdvisor(0);

        proxyFac.addAdvisor(new DefaultPointcutAdvisor(
            new GridifySpringPointcut(GridifySpringPointcut.GridifySpringPointcutType.DFLT), dfltAsp));
        proxyFac.addAdvisor(new DefaultPointcutAdvisor(
            new GridifySpringPointcut(GridifySpringPointcut.GridifySpringPointcutType.SET_TO_VALUE), setToValAsp));
        proxyFac.addAdvisor(new DefaultPointcutAdvisor(
            new GridifySpringPointcut(GridifySpringPointcut.GridifySpringPointcutType.SET_TO_SET), setToSetAsp));

        return (T)proxyFac.getProxy();
    }
}