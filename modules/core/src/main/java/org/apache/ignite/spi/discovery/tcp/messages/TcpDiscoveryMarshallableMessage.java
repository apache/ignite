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

package org.apache.ignite.spi.discovery.tcp.messages;

import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Base class for TCP Discovery messages which still require external pre- and post- serialization.
 * <br>
 * TODO: Remove/revise after https://issues.apache.org/jira/browse/IGNITE-25883
 */
public interface TcpDiscoveryMarshallableMessage extends Message {
    /** @param marsh Marshaller. */
    void prepareMarshal(Marshaller marsh);

    /**
     * @param marsh Marshaller.
     * @param clsLdr Class loader.
     */
    void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr);
}
