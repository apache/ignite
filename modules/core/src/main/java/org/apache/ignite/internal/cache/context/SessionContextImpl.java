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

package org.apache.ignite.internal.cache.context;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.SessionContext;
import org.jetbrains.annotations.Nullable;

/** */
public final class SessionContextImpl implements SessionContext {
    /** Session attributes. */
    private final Map<String, String> attrs;

    /** @param attrs Session attributes. */
    public SessionContextImpl(Map<String, String> attrs) {
        this.attrs = new HashMap<>(attrs);
    }

    /** {@inheritDoc} */
    @Override public @Nullable String getAttribute(String name) {
        return attrs.get(name);
    }

    /** @return Application attributes. */
    public Map<String, String> getAttributes() {
        return attrs;
    }
}
