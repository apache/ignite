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

package org.apache.ignite.internal.processors.service.permission;

import java.security.PermissionCollection;
import org.apache.ignite.internal.processors.security.permission.ActionDefs;
import org.apache.ignite.internal.processors.security.permission.ActionPermission;

public final class ServicePermission extends ActionPermission {
    private static final long serialVersionUID = -261094925868648825L;

    public static final String DEPLOY = "deploy";
    public static final String INVOKE = "invoke";
    public static final String CANCEL = "cancel";
    public static final String ALL = "*";

    private static final int CODE_DEPLOY = 0x1;
    private static final int CODE_INVOKE = 0x2;
    private static final int CODE_CANCEL = 0x4;
    private static final int CODE_ALL = CODE_DEPLOY | CODE_INVOKE | CODE_CANCEL;

    private static final ActionDefs ACTION_DEFS = ActionDefs.builder()
        .add(CODE_DEPLOY, DEPLOY)
        .add(CODE_INVOKE, INVOKE)
        .add(CODE_CANCEL, CANCEL)
        .add(CODE_ALL, ALL)
        .build();

    public ServicePermission(String name, String actions) {
        super(name, actions);
    }

    @Override public PermissionCollection newPermissionCollection() {
        return new ServicePermissionCollection();
    }

    @Override protected ActionDefs actionDefs() {
        return ACTION_DEFS;
    }

    @Override protected int codeAll() {
        return CODE_ALL;
    }
}
