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

package org.apache.ignite.mesos;

import junit.framework.TestCase;
import org.apache.mesos.Protos;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import static org.junit.Assert.assertThat;

/**
 * FrameworkInfoTest
 */
public class IgniteFrameworkInfoTest extends TestCase {

    /** Framework name.*/
    private static final String IGNITE_FRAMEWORK_NAME = "Ignite";

    /** Mesos user name in system environment.*/
    private static final String MESOS_USER_NAME = "MESOS_USER";

    /** */
    private final String testName = "mesosusername";

    /** Mesos user name in system environment.*/
    private static final String MESOS_ROLE = "MESOS_ROLE";

    /** */
    private final String testRole = "mesosrole";

    /** */
    @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        environmentVariables.set(MESOS_USER_NAME, testName);
        environmentVariables.set(MESOS_ROLE, testRole);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFrameworkInfo() throws Exception {

        String name = System.getenv(MESOS_USER_NAME);
        String role = System.getenv(MESOS_ROLE);
        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(IGNITE_FRAMEWORK_NAME)
            .setUser(name != null ? name : "")
            .setRole(role != null ? role : "*");

        assertThat(testName, Is.is(name));
        assertThat(testRole, Is.is(role));

        assertThat(name, Is.is(frameworkBuilder.getUser()));
        assertThat(role, Is.is(frameworkBuilder.getRole()));
        assertThat(IGNITE_FRAMEWORK_NAME, Is.is(frameworkBuilder.getName()));
    }
}