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

import java.util.List;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Transport for {@link OperationContext} distibuted attributes. */
public class DistributedOperationAttributesMessage implements Message {
    /** Values of operation context attributes. */
    @Order(0)
    public List<Message> vals;

    /** Bitmask of effective attributes ids. */
    @Order(1)
    public byte idBitmask;

    /** Empty constructor for serialization purposes. */
    public DistributedOperationAttributesMessage() {
        // No-op.
    }
}
