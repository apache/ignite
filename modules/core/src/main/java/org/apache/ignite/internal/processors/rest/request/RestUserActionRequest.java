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

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.internal.processors.security.UserOptions;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * User request.
 */
public class RestUserActionRequest extends GridRestRequest {
    /** User name. */
    private String user;

    /** User options. */
    @GridToStringExclude
    private UserOptions opts;

    /**
     * @param user User name.
     */
    public void user(String user) {
        this.user = user;
    }

    /**
     * @return User name.
     */
    public String user() {
        return user;
    }

    /** Sets user options. */
    public void userOptions(UserOptions opts) {
        this.opts = opts;
    }

    /** Gets user options. */
    public UserOptions userOptions() {
        return opts;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RestUserActionRequest.class, this, super.toString());
    }
}
