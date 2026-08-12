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
 * Marshals the {@link Marshalled} fields of the message with the JDK marshaller of the local node, whatever
 * marshaller the transport speaks. Put it on a message that travels both transports: the wire form of a
 * {@link Marshalled} field is cached in its companion field, so an instance marshalled by one transport would hand
 * the other transport bytes of a format it does not read. A pinned marshaller also keeps the message off the
 * cluster-wide class name registration, which never completes when the marshalling happens on a discovery thread.
 *
 * @see Marshalled
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
public @interface JdkMarshalled {
}
