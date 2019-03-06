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
    @Override public String getTestIgniteInstanceName() {
        return "TestAopTarget";
    }
}