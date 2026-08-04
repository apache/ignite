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

package org.apache.ignite.internal.processors.query.calcite.message;

import org.apache.ignite.internal.managers.communication.IgniteMessageFactory;
import org.apache.ignite.internal.plugin.AbstractCompanionMessageFactoryProvider;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Message factory.
 */
public class CalciteMessageFactory extends AbstractCompanionMessageFactoryProvider {
    /** */
    public static final short MIN_MESSAGE_TYPE = 300;

    /** */
    public static final short MAX_MESSAGE_TYPE = 311;

    /** {@inheritDoc} */
    @Override public void registerAll(IgniteMessageFactory factory) {
        register(factory, QueryStartRequest.class, (short)300);
        register(factory, QueryStartResponse.class, (short)301);
        register(factory, CalciteErrorMessage.class, (short)302);
        register(factory, QueryBatchMessage.class, (short)303);
        register(factory, QueryBatchAcknowledgeMessage.class, (short)304);
        register(factory, QueryInboxCloseMessage.class, (short)305);
        register(factory, QueryCloseMessage.class, (short)306);
        register(factory, GenericValueMessage.class, (short)307);
        register(factory, FragmentMapping.class, (short)308);
        register(factory, ColocationGroup.class, (short)309);
        register(factory, FragmentDescription.class, (short)310);
        register(factory, QueryTxEntry.class, (short)311);
    }

    /** */
    public static boolean isCalciteMessage(Message msg) {
        return msg.directType() >= MIN_MESSAGE_TYPE && msg.directType() <= MAX_MESSAGE_TYPE;
    }
}
