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

package org.apache.ignite.internal.testframework;

import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy;
import org.apache.ignite.lang.IgniteLogger;

import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.lang.IgniteSystemProperties.getString;

/**
 * Ignite base test class.
 */
public abstract class IgniteAbstractTest {
    /** Logger. */
    protected static IgniteLogger log;

    /** Init test env. */
    static {
        S.setSensitiveDataLoggingPolicySupplier(() ->
            SensitiveDataLoggingPolicy.valueOf(getString(IGNITE_SENSITIVE_DATA_LOGGING, "hash").toUpperCase()));
    }

    /**
     * Constructor.
     */
    @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
    protected IgniteAbstractTest() {
        log = IgniteLogger.forClass(getClass());
    }

    /**
     * @return Logger.
     */
    protected IgniteLogger logger() {
        return log;
    }
}
