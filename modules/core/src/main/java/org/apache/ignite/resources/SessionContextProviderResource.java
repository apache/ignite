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

package org.apache.ignite.resources;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.session.SessionContext;

/**
 * Annotates a field for injecting a {@link SessionContext} into a user defined functions.
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 *     public class MyFunctions {
 *         &#64;SessionContextProviderResource
 *         public SessionContextProvider sesCtxProv;
 *
 *         &#64;QuerySqlFunction
 *         public String sessionId() {
 *             SessionContext sesCtx = sesCtxProv.getSessionContext();
 *
 *             return sesCtx.getAttribute("SESSION_ID");
 *         }
 *     }
 * </pre>
 * <p>
 * Note, {@link CacheInterceptor}, {@link QuerySqlFunction} in the Calcite engine are supported.
 *
 * @see SessionContext
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface SessionContextProviderResource {
    // No-op.
}
