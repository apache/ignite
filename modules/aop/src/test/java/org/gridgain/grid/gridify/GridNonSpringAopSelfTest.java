/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify;

import org.gridgain.testframework.junits.common.*;

/**
 * To run this test with JBoss AOP make sure of the following:
 *
 * 1. The JVM is started with following parameters to enable jboss online weaving
 *      (replace ${GRIDGAIN_HOME} to you $GRIDGAIN_HOME):
 *      -javaagent:${GRIDGAIN_HOME}libs/jboss-aop-jdk50-4.0.4.jar
 *      -Djboss.aop.class.path=[path to grid compiled classes (Idea out folder) or path to gridgain.jar]
 *      -Djboss.aop.exclude=org,com -Djboss.aop.include=org.gridgain
 *
 * 2. The following jars should be in a classpath:
 *      ${GRIDGAIN_HOME}libs/javassist-3.x.x.jar
 *      ${GRIDGAIN_HOME}libs/jboss-aop-jdk50-4.0.4.jar
 *      ${GRIDGAIN_HOME}libs/jboss-aspect-library-jdk50-4.0.4.jar
 *      ${GRIDGAIN_HOME}libs/jboss-common-4.2.2.jar
 *      ${GRIDGAIN_HOME}libs/trove-1.0.2.jar
 *
 * To run this test with AspectJ AOP make sure of the following:
 *
 * 1. The JVM is started with following parameters for enable AspectJ online weaving
 *      (replace ${GRIDGAIN_HOME} to you $GRIDGAIN_HOME):
 *      -javaagent:${GRIDGAIN_HOME}/libs/aspectjweaver-1.7.2.jar
 *
 * 2. Classpath should contains the ${GRIDGAIN_HOME}/modules/tests/config/aop/aspectj folder.
 */
@GridCommonTest(group="AOP")
public class GridNonSpringAopSelfTest extends GridAbstractAopTest {
    /** {@inheritDoc} */
    @Override protected Object target() {
        return new GridTestAopTarget();
    }

    /** {@inheritDoc} */
    @Override public String getTestGridName() {
        return "GridTestAopTarget";
    }
}
