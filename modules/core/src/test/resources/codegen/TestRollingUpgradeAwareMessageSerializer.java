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

import org.apache.ignite.internal.MessageSerializationContext;
import org.apache.ignite.internal.TestFeatureRegistry;
import org.apache.ignite.internal.TestRollingUpgradeAwareMessage;
import org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public final class TestRollingUpgradeAwareMessageSerializer implements MessageSerializer<TestRollingUpgradeAwareMessage> {
    /** */
    @Override public final boolean writeTo(TestRollingUpgradeAwareMessage msg, MessageWriter writer, MessageSerializationContext ctx) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt(msg.plain))
                    return false;

                writer.incrementState();

            case 1:
                if (ctx.includeFieldDeprecatedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE)) {
                    if (!writer.writeString(msg.oldFld))
                        return false;
                }

                writer.incrementState();

            case 2:
                if (ctx.includeFieldIntroducedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE)) {
                    if (!writer.writeString(msg.newFld))
                        return false;
                }

                writer.incrementState();

            case 3:
                if (ctx.includeFieldIntroducedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE) && ctx.includeFieldDeprecatedBy(TestFeatureRegistry.SECOND_FEATURE)) {
                    if (!writer.writeLong(msg.windowed))
                        return false;
                }

                writer.incrementState();

        }

        return true;
    }

    /** */
    @Override public final boolean readFrom(TestRollingUpgradeAwareMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        switch (reader.state()) {
            case 0:
                msg.plain = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                if (ctx.includeFieldDeprecatedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE)) {
                    msg.oldFld = reader.readString();

                    if (!reader.isLastRead())
                        return false;
                }

                reader.incrementState();

            case 2:
                if (ctx.includeFieldIntroducedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE)) {
                    msg.newFld = reader.readString();

                    if (!reader.isLastRead())
                        return false;
                }

                reader.incrementState();

            case 3:
                if (ctx.includeFieldIntroducedBy(SupportedFeatureRegistry.ROLLING_UPGRADE_FEATURE) && ctx.includeFieldDeprecatedBy(TestFeatureRegistry.SECOND_FEATURE)) {
                    msg.windowed = reader.readLong();

                    if (!reader.isLastRead())
                        return false;
                }

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final TestRollingUpgradeAwareMessage createMessage() {
        return new TestRollingUpgradeAwareMessage();
    }
}
