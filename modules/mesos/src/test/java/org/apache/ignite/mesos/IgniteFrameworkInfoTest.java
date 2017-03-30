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

import com.google.protobuf.ByteString;
import junit.framework.TestCase;
import org.apache.mesos.Protos;
import org.hamcrest.core.Is;
import static org.junit.Assert.assertThat;

/**
 * FrameworkInfoTest.
 */
public class IgniteFrameworkInfoTest extends TestCase {
    /** Framework name. */
    private static final String IGNITE_FRAMEWORK_NAME = "Ignite";

    /** User name. */
    private static final String MESOS_USER_NAME = "MESOS_USER";

    /** Mesos role name. */
    private static final String MESOS_ROLE = "MESOS_ROLE";

    /**
     * @throws Exception If failed.
     */
    public void testFrameworkInfo() throws Exception {
        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(IGNITE_FRAMEWORK_NAME)
            .setUser(MESOS_USER_NAME)
            .setUserBytes(ByteString.copyFromUtf8(MESOS_USER_NAME))
            .setRole(MESOS_ROLE)
            .setRoleBytes(ByteString.copyFromUtf8(MESOS_ROLE));

        Protos.FrameworkInfo protos = frameworkBuilder.build();

        assertThat(IGNITE_FRAMEWORK_NAME, Is.is(protos.getName()));
        assertThat(MESOS_USER_NAME, Is.is(protos.getUser()));
        assertThat(MESOS_USER_NAME, Is.is(protos.getUserBytes().toStringUtf8()));
        assertThat(MESOS_ROLE, Is.is(protos.getRole()));
        assertThat(MESOS_ROLE, Is.is(protos.getRoleBytes().toStringUtf8()));
    }
}