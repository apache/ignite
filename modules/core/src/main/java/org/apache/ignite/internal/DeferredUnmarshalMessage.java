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

import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * A {@link Message} whose payload is unmarshalled by the consumer on its own execution path instead of the generic
 * inbound pass of {@code GridIoManager}. The consumer is responsible for unmarshalling every remotely received
 * message exactly once, and gains what the generic pass cannot offer: the proper thread pool instead of a NIO
 * thread (Calcite messages in the query task executor), the peer-deployment class loader able to see user classes
 * instead of the configuration one, and a protocol-level failure response to the sender (cache messages in
 * {@code GridCacheIoManager}, data streamer entries in the update job, continuous-query entries in
 * {@code CacheContinuousQueryHandler}).
 */
public interface DeferredUnmarshalMessage extends Message {
}
