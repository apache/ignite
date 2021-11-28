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

package org.apache.ignite.internal.table.distributed.command;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * This is an utility class for serialization cache tuples. It will be removed after another way for serialization is implemented into the
 * network layer.
 * TODO: Remove it after (IGNITE-14793)
 */
public class CommandUtils {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(CommandUtils.class);

    /**
     * Writes a list of rows to byte array.
     *
     * @param rows     Collection of rows.
     * @return         Rows data.
     */
    public static byte[] rowsToBytes(Collection<BinaryRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return null;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            for (BinaryRow row : rows) {
                if (row == null) {
                    baos.write(intToBytes(0));
                } else {
                    byte[] bytes = rowToBytes(row);

                    baos.write(intToBytes(bytes.length));
                    baos.write(bytes);
                }
            }

            baos.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            LOG.error("Could not write rows to stream [rows=" + rows.size() + ']', e);

            throw new IgniteInternalException(e);
        }
    }

    /**
     * Writes a row to byte array.
     *
     * @param row      Row.
     * @return         Row bytes.
     */
    public static byte[] rowToBytes(@Nullable BinaryRow row) {
        if (row == null) {
            return null;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            row.writeTo(baos);

            baos.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            LOG.error("Could not write row to stream [row=" + row + ']', e);

            throw new IgniteInternalException(e);
        }
    }

    /**
     * Reads the keys from a byte array.
     *
     * @param bytes    Byte array.
     * @param consumer Consumer for binary row.
     */
    public static void readRows(byte[] bytes, Consumer<BinaryRow> consumer) {
        if (bytes == null || bytes.length == 0) {
            return;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            byte[] lenBytes = new byte[4];

            byte[] rowBytes;

            int read;

            while ((read = bais.read(lenBytes)) != -1) {
                assert read == 4;

                int len = bytesToInt(lenBytes);

                if (len == 0) {
                    consumer.accept(null);

                    continue;
                }

                rowBytes = new byte[len];

                read = bais.read(rowBytes);

                assert read == len;

                consumer.accept(new ByteBufferRow(rowBytes));
            }
        } catch (IOException e) {
            LOG.error("Could not read rows from stream.", e);
        }
    }

    /**
     * Serializes an integer to the byte array.
     *
     * @param i Integer value.
     * @return Byte array.
     */
    private static byte[] intToBytes(int i) {
        byte[] arr = new byte[4];
        ByteBuffer.wrap(arr).putInt(i);
        return arr;
    }

    /**
     * Deserializes a byte array to the integer.
     *
     * @param bytes Byte array.
     * @return Integer value.
     */
    private static int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }
}
