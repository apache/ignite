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

import org.apache.ignite.internal.plugin.AbstractMarshallableMessageFactoryProvider;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/**
 * Message factory.
 */
public class CalciteMessageFactory extends AbstractMarshallableMessageFactoryProvider {
    /** */
    private static CalciteMessageFactory INSTANCE;

    /** */
    public static final short MIN_MESSAGE_TYPE = 300;

    /** */
    private short msgType = MIN_MESSAGE_TYPE;

    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        assert INSTANCE == null : "Calcite message factory is already initialized.";

        register(factory, QueryStartRequest.class, msgType++, schemaAwareMarsh, resolvedClsLdr);
        register(factory, QueryStartResponse.class, msgType++, dfltMarsh, dftlClsLdr);
        register(factory, CalciteErrorMessage.class, msgType++, dfltMarsh, resolvedClsLdr);
        register(factory, QueryBatchMessage.class, msgType++, dfltMarsh, dftlClsLdr);
        register(factory, QueryBatchAcknowledgeMessage.class, msgType++, dfltMarsh, dftlClsLdr);
        register(factory, QueryInboxCloseMessage.class, msgType++, dfltMarsh, dftlClsLdr);
        register(factory, QueryCloseMessage.class, msgType++, dfltMarsh, dftlClsLdr);
        register(factory, GenericValueMessage.class, msgType++, schemaAwareMarsh, resolvedClsLdr);
        register(factory, FragmentMapping.class, msgType++, dfltMarsh, dftlClsLdr);
        register(factory, ColocationGroup.class, msgType++, dfltMarsh, dftlClsLdr);
        register(factory, FragmentDescription.class, msgType++, dfltMarsh, dftlClsLdr);
        register(factory, QueryTxEntry.class, msgType++, dfltMarsh, dftlClsLdr);

        INSTANCE = this;
    }

    /** */
    public static boolean isCalciteMessage(Message msg) {
        assert INSTANCE != null : "Calcite message factory is not initialized yet.";

        return msg.directType() >= MIN_MESSAGE_TYPE && msg.directType() < INSTANCE.msgType;
    }
}
