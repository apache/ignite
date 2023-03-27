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

package org.apache.ignite.spi.discovery;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is for all implementations of {@link DiscoverySpi} that support
 * proper node ordering. This includes:
 * <ul>
 * <li>
 * Every node gets an order number assigned to it which is provided via {@link org.apache.ignite.cluster.ClusterNode#order()}
 * method. There is no requirement about order value other than that nodes that join grid
 * at later point of time have order values greater than previous nodes.
 * </li>
 * <li>
 * All {@link org.apache.ignite.events.EventType#EVT_NODE_JOINED} events come in proper order. This means that all
 * listeners to discovery events will receive discovery notifications in proper order.
 * </li>
 * </ul>
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DiscoverySpiOrderSupport {
    /**
     * @return Whether or not target SPI supports node startup order.
     */
    public boolean value();
}
