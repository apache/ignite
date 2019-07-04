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

package org.apache.ignite.internal.processors.task.permission;

import java.security.PermissionCollection;
import org.apache.ignite.internal.processors.security.permission.ActionDefs;
import org.apache.ignite.internal.processors.security.permission.ActionPermission;

public class TaskPermission extends ActionPermission {
    private static final long serialVersionUID = -1495607228802790333L;

    /** Execute action. */
    public static final String EXECUTE = "execute";

    /** Cancel action. */
    public static final String CANCEL = "cancel";

    /** All actions (execute, cancel). */
    public static final String ALL = "*";

    /** Execute action. */
    private static final int CODE_EXECUTE = 0x1;

    /** Cancel action. */
    private static final int CODE_CANCEL = 0x2;

    /** All actions (execute, cancel). */
    private static final int CODE_ALL = CODE_EXECUTE | CODE_CANCEL;

    private static final ActionDefs ACTION_DEFS = ActionDefs.builder()
        .add(CODE_EXECUTE, EXECUTE)
        .add(CODE_CANCEL, CANCEL)
        .add(CODE_ALL, ALL)
        .build();

    public TaskPermission(String name, String actions) {
        super(name, actions);
    }

    /**
     * Returns a new PermissionCollection object for storing TaskPermission objects.
     * <p>
     *
     * @return a new PermissionCollection object suitable for storing TaskPermissions.
     */
    @Override public PermissionCollection newPermissionCollection() {
        return new TaskPermissionCollection();
    }

    @Override protected int codeAll() {
        return CODE_ALL;
    }

    @Override protected ActionDefs actionDefs() {
        return ACTION_DEFS;
    }
}
