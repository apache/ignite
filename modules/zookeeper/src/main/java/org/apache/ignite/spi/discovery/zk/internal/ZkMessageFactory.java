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

package org.apache.ignite.spi.discovery.zk.internal;

import org.apache.ignite.internal.plugin.AbstractMarshallableMessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** */
public class ZkMessageFactory extends AbstractMarshallableMessageFactoryProvider {
    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        register(factory, ZkCommunicationErrorResolveFinishMessage.class, (short)400, ZkCommunicationErrorResolveFinishMessage::new, dfltMarsh);
        register(factory, ZkCommunicationErrorResolveStartMessage.class, (short)401, ZkCommunicationErrorResolveStartMessage::new, dfltMarsh);
        register(factory, ZkForceNodeFailMessage.class, (short)402, ZkForceNodeFailMessage::new, dfltMarsh);
        register(factory, ZkNoServersMessage.class, (short)403, ZkNoServersMessage::new, dfltMarsh);
        register(factory, ZkDiscoDataBagWrapper.class, (short)404, ZkDiscoDataBagWrapper::new, dfltMarsh);
    }
}
