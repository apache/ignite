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

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.cache.ApplicationContext;

public class ApplicationContextInternal implements ApplicationContext, AutoCloseable {
    /** */
    private final ApplicationContextProcessor proc;

    /** Application attributes. */
    private final Map<String, String> attrs;

    /** @param attrs Application attributes. */
    public ApplicationContextInternal(ApplicationContextProcessor proc, Map<String, String> attrs) {
        this.attrs = attrs;
        this.proc = proc;
    }

    /** */
    @Override public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attrs);
    }

    /** Unset context for current thread. */
    @Override public void close() {
        proc.clean();
    }
}
