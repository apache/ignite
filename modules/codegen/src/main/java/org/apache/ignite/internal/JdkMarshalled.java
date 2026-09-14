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

package org.apache.ignite.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marshals the message with the JDK marshaller of the local node, whatever marshaller the transport uses. Put it on a
 * message that travels both transports: a {@link Marshalled} field caches its wire form, and only the JDK marshaller
 * is read by both. It also asks for no cluster-wide type registration, which a discovery thread cannot wait
 * for: the answer comes back through discovery, which that very thread has to move forward.
 *
 * @see Marshalled
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
public @interface JdkMarshalled {
}
