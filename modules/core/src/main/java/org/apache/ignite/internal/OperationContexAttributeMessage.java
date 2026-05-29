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

import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Transport of {@link OperationContextAttribute}. */
public class OperationContexAttributeMessage implements Message {
    /** Operation context attribute type.  */
    @Order(0)
    OperationContextAttributeType type;

    /** Operation context attribute value. */
    @Order(1)
    Message val;

    /** Empty constructor for serialization purposes. */
    public OperationContexAttributeMessage() {
        // No-op.
    }

    /** Creates operation context attribute message. */
    public OperationContexAttributeMessage(OperationContextAttributeType type, Message val) {
        this.type = type;
        this.val = val;
    }
}
