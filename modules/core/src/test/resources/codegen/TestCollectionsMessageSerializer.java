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
import org.apache.ignite.internal.TestCollectionsMessage;
import org.apache.ignite.plugin.extensions.communication.CollectionImplementationType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public final class TestCollectionsMessageSerializer implements MessageSerializer<TestCollectionsMessage> {
    /** */
    private static final MessageCollectionType affTopVersionListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType bitSetListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType bitSetSetCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), CollectionImplementationType.HASH_SET);
    /** */
    private static final MessageCollectionType booleanArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType boxedBooleanListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType boxedByteListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType boxedCharListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType boxedDoubleListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType boxedFloatListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType boxedIntListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType boxedIntegerSetCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), CollectionImplementationType.HASH_SET);
    /** */
    private static final MessageCollectionType boxedLongListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType boxedShortListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType byteArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType cacheObjectSetCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CACHE_OBJECT), CollectionImplementationType.HASH_SET);
    /** */
    private static final MessageCollectionType charArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType doubleArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType floatArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType gridLongListListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType igniteUuidListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType intArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT_ARR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType longArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG_ARR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType messageListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.GRID_CACHE_VERSION), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType shortArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType stringListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.STRING), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageCollectionType uuidListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.UUID), CollectionImplementationType.ARRAY_LIST);

    /** */
    @Override public final boolean writeTo(TestCollectionsMessage msg, MessageWriter writer, MessageSerializationContext ctx) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(msg.booleanArrayList, booleanArrayListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection(msg.byteArrayList, byteArrayListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection(msg.shortArrayList, shortArrayListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection(msg.intArrayList, intArrayListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection(msg.longArrayList, longArrayListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection(msg.charArrayList, charArrayListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection(msg.floatArrayList, floatArrayListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection(msg.doubleArrayList, doubleArrayListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection(msg.stringList, stringListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection(msg.uuidList, uuidListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection(msg.bitSetList, bitSetListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection(msg.igniteUuidList, igniteUuidListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection(msg.affTopVersionList, affTopVersionListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCollection(msg.boxedBooleanList, boxedBooleanListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection(msg.boxedByteList, boxedByteListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection(msg.boxedShortList, boxedShortListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeCollection(msg.boxedIntList, boxedIntListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeCollection(msg.boxedLongList, boxedLongListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeCollection(msg.boxedCharList, boxedCharListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeCollection(msg.boxedFloatList, boxedFloatListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeCollection(msg.boxedDoubleList, boxedDoubleListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeCollection(msg.messageList, messageListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeCollection(msg.gridLongListList, gridLongListListCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeCollection(msg.boxedIntegerSet, boxedIntegerSetCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection(msg.bitSetSet, bitSetSetCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeCollection(msg.cacheObjectSet, cacheObjectSetCollDesc, ctx))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public final boolean readFrom(TestCollectionsMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        switch (reader.state()) {
            case 0:
                msg.booleanArrayList = reader.readCollection(booleanArrayListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArrayList = reader.readCollection(byteArrayListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.shortArrayList = reader.readCollection(shortArrayListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.intArrayList = reader.readCollection(intArrayListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.longArrayList = reader.readCollection(longArrayListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.charArrayList = reader.readCollection(charArrayListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.floatArrayList = reader.readCollection(floatArrayListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.doubleArrayList = reader.readCollection(doubleArrayListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.stringList = reader.readCollection(stringListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.uuidList = reader.readCollection(uuidListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSetList = reader.readCollection(bitSetListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.igniteUuidList = reader.readCollection(igniteUuidListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.affTopVersionList = reader.readCollection(affTopVersionListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.boxedBooleanList = reader.readCollection(boxedBooleanListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.boxedByteList = reader.readCollection(boxedByteListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                msg.boxedShortList = reader.readCollection(boxedShortListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                msg.boxedIntList = reader.readCollection(boxedIntListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                msg.boxedLongList = reader.readCollection(boxedLongListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                msg.boxedCharList = reader.readCollection(boxedCharListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                msg.boxedFloatList = reader.readCollection(boxedFloatListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                msg.boxedDoubleList = reader.readCollection(boxedDoubleListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                msg.messageList = reader.readCollection(messageListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                msg.gridLongListList = reader.readCollection(gridLongListListCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                msg.boxedIntegerSet = reader.readCollection(boxedIntegerSetCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                msg.bitSetSet = reader.readCollection(bitSetSetCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                msg.cacheObjectSet = reader.readCollection(cacheObjectSetCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final TestCollectionsMessage createMessage() {
        return new TestCollectionsMessage();
    }
}