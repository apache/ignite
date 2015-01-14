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

package org.apache.ignite.spi.authentication;

import org.apache.ignite.plugin.security.*;

import java.net.*;
import java.util.*;

/**
 * Authentication context.
 */
public interface AuthenticationContext {
    /**
     * Gets subject type.
     *
     * @return Subject type.
     */
    public GridSecuritySubjectType subjectType();

    /**
     * Gets subject ID.
     *
     * @return Subject ID.
     */
    public UUID subjectId();

    /**
     * Gets security credentials.
     *
     * @return Security credentials.
     */
    public GridSecurityCredentials credentials();

    /**
     * Gets subject network address.
     *
     * @return Subject network address.
     */
    public InetSocketAddress address();
}
