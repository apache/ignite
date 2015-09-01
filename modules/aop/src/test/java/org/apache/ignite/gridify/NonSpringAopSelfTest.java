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

package org.apache.ignite.gridify;

import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * To run this test with JBoss AOP make sure of the following:
 *
 * 1. The JVM is started with following parameters to enable jboss online weaving
 *      (replace ${IGNITE_HOME} to you $IGNITE_HOME):
 *      -javaagent:${IGNITE_HOME}libs/jboss-aop-jdk50-4.0.4.jar
 *      -Djboss.aop.class.path=[path to grid compiled classes (Idea out folder) or path to ignite.jar]
 *      -Djboss.aop.exclude=org,com -Djboss.aop.include=org.apache.ignite
 *
 * 2. The following jars should be in a classpath:
 *      ${IGNITE_HOME}libs/javassist-3.x.x.jar
 *      ${IGNITE_HOME}libs/jboss-aop-jdk50-4.0.4.jar
 *      ${IGNITE_HOME}libs/jboss-aspect-library-jdk50-4.0.4.jar
 *      ${IGNITE_HOME}libs/jboss-common-4.2.2.jar
 *      ${IGNITE_HOME}libs/trove-1.0.2.jar
 *
 * To run this test with AspectJ AOP make sure of the following:
 *
 * 1. The JVM is started with following parameters for enable AspectJ online weaving
 *      (replace ${IGNITE_HOME} to you $IGNITE_HOME):
 *      -javaagent:${IGNITE_HOME}/libs/optional/ignite-aop/aspectjweaver-1.7.2.jar
 *
 * 2. Classpath should contains the ${IGNITE_HOME}/modules/tests/config/aop/aspectj folder.
 */
@GridCommonTest(group="AOP")
public class NonSpringAopSelfTest extends AbstractAopTest {
    /** {@inheritDoc} */
    @Override protected Object target() {
        return new TestAopTarget();
    }

    /** {@inheritDoc} */
    @Override public String getTestGridName() {
        return "TestAopTarget";
    }
}