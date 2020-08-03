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

package org.apache.ignite.internal.processors.security.sandbox;

import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * IgniteSandbox executes a user-defined code with restrictions.
 */
public interface IgniteSandbox {
    /**
     * Executes {@code callable} with constraints defined by current {@code SecuritySubject}.
     *
     * @param call Callable to execute.
     * @return Result of {@code callable}.
     * @see IgniteSecurity#withContext(UUID)
     * @see IgniteSecurity#withContext(SecurityContext)
     * @see SecuritySubject#sandboxPermissions()
     */
    public <T> T execute(Callable<T> call) throws IgniteException;

    /**
     * @return True if the sandbox is enabled.
     */
    public boolean enabled();
}
