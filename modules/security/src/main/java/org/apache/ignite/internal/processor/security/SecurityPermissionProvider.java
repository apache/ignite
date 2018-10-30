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

package org.apache.ignite.internal.processor.security;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 *
 */
public class SecurityPermissionProvider {
    /** Permission set map. */
    private static final Map<Object, SecurityPermissionSet> PERMISSION_SET_MAP = new HashMap<>();

    /**
     * @param tok Token.
     */
    public static SecurityPermissionSet permission(Object tok) {
        return PERMISSION_SET_MAP.get(tok);
    }

    /**
     * @param permSet Permission set.
     * @param tokens Tokens.
     */
    public static void add(SecurityPermissionSet permSet, Object... tokens) {
        for (Object t : tokens)
            PERMISSION_SET_MAP.put(t, permSet);
    }

    /**
     * Clear map of permissions.
     */
    public static void clear() {
        PERMISSION_SET_MAP.clear();
    }
}
