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

package org.apache.ignite.lang;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotations should be used to mark any type that should not be
 * peer deployable. Peer deployment will fail for this object as if
 * class could not be found.
 * <p>
 * This annotation is used as <b>non-distribution assertion</b> and should be
 * applied to classes and interfaces that should never be distributed via
 * peer-to-peer deployment.
 * <p>
 * Note, however, that if class is already available on the remote node it
 * will not be peer-loaded but will simply be locally class loaded. It may appear
 * as if it was successfully peer-loaded when in fact it was simply already
 * available on the remote node.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface IgniteNotPeerDeployable {
    // No-op.
}