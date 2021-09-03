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

package org.apache.ignite.network;

import org.apache.ignite.internal.network.processor.EmptyMessage;
import org.apache.ignite.internal.network.processor.InheritedMessage;
import org.apache.ignite.internal.network.processor.SerializationOrderMessage;
import org.apache.ignite.network.annotations.MessageGroup;

@MessageGroup(groupName = "TestMessages", groupType = 2)
public class TestMessageTypes {
    /** Type of {@link SerializationOrderMessage} */
    public static final short SERIALIZATION_ORDER_MESSAGE = 1;

    /** Type of {@link InheritedMessage} */
    public static final short INHERITED_MESSAGE = 2;

    /** Type of {@link EmptyMessage} */
    public static final short EMPTY_MESSAGE = 3;

    /** */
    public static final short ALL_TYPES = 4;

    /** */
    public static final short TEST = 5;

    /** */
    public static final short NESTED_MESSAGE = 6;
}
