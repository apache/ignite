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

package org.apache.ignite.internal.processors.security;

import static sun.security.util.SecurityConstants.MODIFY_THREADGROUP_PERMISSION;
import static sun.security.util.SecurityConstants.MODIFY_THREAD_PERMISSION;

/**
 * Default Ignite Security Manager.
 */
public class IgniteSecurityManager extends SecurityManager {
    /** */
    @Override public void checkAccess(Thread t) {
        super.checkAccess(t);

        checkPermission(MODIFY_THREAD_PERMISSION);
    }

    /** */
    @Override public void checkAccess(ThreadGroup g) {
        super.checkAccess(g);

        checkPermission(MODIFY_THREADGROUP_PERMISSION);
    }
}
