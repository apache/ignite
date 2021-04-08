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

import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.ignite.internal.processors.security.UserOptions.UserOption;
import static org.apache.ignite.internal.processors.security.UserOptions.UserOption.PASSWORD;

/** Represents storage for security user options. */
public class UserOptions implements Iterable<Map.Entry<UserOption, Object>> {
    /** User options mapped to their aliases. */
    private final Map<UserOption, Object> opts = new EnumMap<>(UserOption.class);

    /** Gets password option. */
    public <T> T password() {
        return (T)opts.get(PASSWORD);
    }

    /** Sets password option. */
    public UserOptions password(Object pwd) {
        opts.put(PASSWORD, pwd);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<UserOption, Object>> iterator() {
        return opts.entrySet().iterator();
    }

    /** Aliases of supported options for create/alter user operations. */
    public enum UserOption {
        /** */
        PASSWORD
    }
}
