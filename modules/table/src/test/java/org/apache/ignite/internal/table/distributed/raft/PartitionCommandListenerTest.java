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

package org.apache.ignite.internal.table.distributed.raft;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorage;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndUpsertCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceIfExistCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.MultiRowsResponse;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the table command listener.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class PartitionCommandListenerTest {
    /** Key count. */
    public static final int KEY_COUNT = 100;

    /** Work directory. */
    @WorkDirectory
    private Path dataPath;

    /** Schema. */
    public static SchemaDescriptor SCHEMA = new SchemaDescriptor(
        1,
        new Column[] {new Column("key", NativeTypes.INT32, false)},
        new Column[] {new Column("value", NativeTypes.INT32, false)}
    );

    /** Table command listener. */
    private PartitionListener commandListener;

    /**
     * Initializes a table listener before tests.
     */
    @BeforeEach
    public void before() {
        commandListener = new PartitionListener(new RocksDbStorage(dataPath.resolve("db"), ByteBuffer::compareTo));
    }

    /**
     * Inserts rows and checks them.
     */
    @Test
    public void testInsertCommands() {
        readAndCheck(false);

        delete(false);

        insert(false);

        insert(true);

        readAndCheck(true);

        delete(true);
    }

    /**
     * Upserts rows and checks them.
     */
    @Test
    public void testUpsertValues() {
        readAndCheck(false);

        upsert();

        readAndCheck(true);

        delete(true);

        readAndCheck(false);
    }

    /**
     * Adds rows, replaces and checks them.
     */
    @Test
    public void testReplaceCommand() {
        upsert();

        deleteExactValues(false);

        replaceValues(true);

        readAndCheck(true, i -> i + 1);

        replaceValues(false);

        readAndCheck(true, i -> i + 1);

        deleteExactValues(true);

        readAndCheck(false);
    }

    /**
     * The test checks PutIfExist command.
     */
    @Test
    public void testPutIfExistCommand() {
        putIfExistValues(false);

        readAndCheck(false);

        upsert();

        putIfExistValues(true);

        readAndCheck(true, i -> i + 1);

        getAndDeleteValues(true);

        readAndCheck(false);

        getAndDeleteValues(false);
    }

    /**
     * The test checks GetAndReplace command.
     */
    @Test
    public void testGetAndReplaceCommand() {
        readAndCheck(false);

        getAndUpsertValues(false);

        readAndCheck(true);

        getAndReplaceValues(true);

        readAndCheck(true, i -> i + 1);

        getAndUpsertValues(true);

        readAndCheck(true);

        deleteExactAllValues(true);

        readAndCheck(false);

        getAndReplaceValues(false);

        deleteExactAllValues(false);
    }

    /**
     * The test checks a batch upsert command.
     */
    @Test
    public void testUpsertRowsBatchedAndCheck() {
        readAll(false);

        deleteAll(false);

        upsertAll();

        readAll(true);

        deleteAll(true);

        readAll(false);
    }

    /**
     * The test checks a batch insert command.
     */
    @Test
    public void testInsertRowsBatchedAndCheck() {
        readAll(false);

        deleteAll(false);

        insertAll(false);

        readAll(true);

        insertAll(true);

        deleteAll(true);

        readAll(false);
    }

    /**
     * Prepares a closure iterator for a specific batch operation.
     *
     * @param func The function prepare a closure for the operation.
     * @param <T> Type of the operation.
     * @return Closure iterator.
     */
    private <T extends Command> Iterator<CommandClosure<T>> batchIterator(Consumer<CommandClosure<T>> func) {
        return new Iterator<CommandClosure<T>>() {
            boolean moved;

            @Override public boolean hasNext() {
                return !moved;
            }

            @Override public CommandClosure<T> next() {
                CommandClosure<T> clo = mock(CommandClosure.class);

                func.accept(clo);

                moved = true;

                return clo;
            }
        };
    }

    /**
     * Prepares a closure iterator for a specific operation.
     *
     * @param func The function prepare a closure for the operation.
     * @param <T> Type of the operation.
     * @return Closure iterator.
     */
    private <T extends Command> Iterator<CommandClosure<T>> iterator(BiConsumer<Integer, CommandClosure<T>> func) {
        return new Iterator<CommandClosure<T>>() {
            /** Iteration. */
            private int i = 0;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return i < KEY_COUNT;
            }

            /** {@inheritDoc} */
            @Override public CommandClosure<T> next() {
                CommandClosure<T> clo = mock(CommandClosure.class);

                func.accept(i, clo);

                i++;

                return clo;
            }
        };
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void insertAll(boolean existed) {
        commandListener.onWrite(batchIterator(clo -> {
            doAnswer(invocation -> {
                MultiRowsResponse resp = invocation.getArgument(0);

                if (existed) {
                    assertEquals(KEY_COUNT, resp.getValues().size());

                    for (BinaryRow binaryRow : resp.getValues()) {
                        Row row = new Row(SCHEMA, binaryRow);

                        int keyVal = row.intValue(0);

                        assertTrue(keyVal < KEY_COUNT);
                        assertEquals(keyVal, row.intValue(1));
                    }
                }
                else
                    assertTrue(resp.getValues().isEmpty());

                return null;
            }).when(clo).result(any(MultiRowsResponse.class));

            Set<BinaryRow> rows = new HashSet<>(KEY_COUNT);

            for (int i = 0; i < KEY_COUNT; i++)
                rows.add(getTestRow(i, i));

            when(clo.command()).thenReturn(new InsertAllCommand(rows));
        }));
    }

    /**
     * Upserts values from the listener in the batch operation.
     */
    private void upsertAll() {
        commandListener.onWrite(batchIterator(clo -> {
            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());

            Set<BinaryRow> rows = new HashSet<>(KEY_COUNT);

            for (int i = 0; i < KEY_COUNT; i++)
                rows.add(getTestRow(i, i));

            when(clo.command()).thenReturn(new UpsertAllCommand(rows));
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void deleteAll(boolean existed) {
        commandListener.onWrite(batchIterator(clo -> {
            doAnswer(invocation -> {
                MultiRowsResponse resp = invocation.getArgument(0);

                if (!existed) {
                    assertEquals(KEY_COUNT, resp.getValues().size());

                    for (BinaryRow binaryRow : resp.getValues()) {
                        Row row = new Row(SCHEMA, binaryRow);

                        int keyVal = row.intValue(0);

                        assertTrue(keyVal < KEY_COUNT);
                    }
                }
                else
                    assertTrue(resp.getValues().isEmpty());

                return null;
            }).when(clo).result(any(MultiRowsResponse.class));

            Set<BinaryRow> keyRows = new HashSet<>(KEY_COUNT);

            for (int i = 0; i < KEY_COUNT; i++)
                keyRows.add(getTestKey(i));

            when(clo.command()).thenReturn(new DeleteAllCommand(keyRows));
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void readAll(boolean existed) {
        commandListener.onRead(batchIterator(clo -> {
            doAnswer(invocation -> {
                MultiRowsResponse resp = invocation.getArgument(0);

                if (existed) {
                    assertEquals(KEY_COUNT, resp.getValues().size());

                    for (BinaryRow binaryRow : resp.getValues()) {
                        Row row = new Row(SCHEMA, binaryRow);

                        int keyVal = row.intValue(0);

                        assertTrue(keyVal < KEY_COUNT);
                        assertEquals(keyVal, row.intValue(1));
                    }
                }
                else
                    assertTrue(resp.getValues().isEmpty());

                return null;
            }).when(clo).result(any(MultiRowsResponse.class));

            Set<BinaryRow> keyRows = new HashSet<>(KEY_COUNT);

            for (int i = 0; i < KEY_COUNT; i++)
                keyRows.add(getTestKey(i));

            when(clo.command()).thenReturn(new GetAllCommand(keyRows));
        }));
    }

    /**
     * Upserts rows.
     */
    private void upsert() {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new UpsertCommand(getTestRow(i, i)));

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void delete(boolean existed) {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new DeleteCommand(getTestKey(i)));

            doAnswer(invocation -> {
                assertEquals(existed, invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * Reads rows from the listener and checks them.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void readAndCheck(boolean existed) {
        readAndCheck(existed, i -> i);
    }

    /**
     * Reads rows from the listener and checks values as expected by a mapper.
     *
     * @param existed True if rows are existed, false otherwise.
     * @param keyValueMapper Mapper a key to the value which will be expected.
     */
    private void readAndCheck(boolean existed, Function<Integer, Integer> keyValueMapper) {
        commandListener.onRead(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new GetCommand(getTestKey(i)));

            doAnswer(invocation -> {
                SingleRowResponse resp = invocation.getArgument(0);

                if (existed) {
                    assertNotNull(resp.getValue());

                    assertTrue(resp.getValue().hasValue());

                    Row row = new Row(SCHEMA, resp.getValue());

                    assertEquals(i, row.intValue(0));
                    assertEquals(keyValueMapper.apply(i), row.intValue(1));
                }
                else
                    assertNull(resp.getValue());

                return null;
            }).when(clo).result(any(SingleRowResponse.class));
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void insert(boolean existed) {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new InsertCommand(getTestRow(i, i)));

            doAnswer(mock -> {
                assertEquals(!existed, mock.getArgument(0));

                return null;
            }).when(clo).result(!existed);
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void deleteExactAllValues(boolean existed) {
        commandListener.onWrite(batchIterator(clo -> {
            HashSet rows = new HashSet(KEY_COUNT);

            for (int i = 0; i < KEY_COUNT; i++)
                rows.add(getTestRow(i, i));

            when(clo.command()).thenReturn(new DeleteExactAllCommand(rows));

            doAnswer(invocation -> {
                MultiRowsResponse resp = invocation.getArgument(0);

                if (!existed) {
                    assertEquals(KEY_COUNT, resp.getValues().size());

                    for (BinaryRow binaryRow : resp.getValues()) {
                        Row row = new Row(SCHEMA, binaryRow);

                        int keyVal = row.intValue(0);

                        assertTrue(keyVal < KEY_COUNT);

                        assertEquals(keyVal, row.intValue(1));
                    }
                }
                else
                    assertTrue(resp.getValues().isEmpty());

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void getAndReplaceValues(boolean existed) {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new GetAndReplaceCommand(getTestRow(i, i + 1)));

            doAnswer(invocation -> {
                SingleRowResponse resp = invocation.getArgument(0);

                if (existed) {
                    Row row = new Row(SCHEMA, resp.getValue());

                    assertEquals(i, row.intValue(0));
                    assertEquals(i, row.intValue(1));
                }
                else
                    assertNull(resp.getValue());

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void getAndUpsertValues(boolean existed) {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new GetAndUpsertCommand(getTestRow(i, i)));

            doAnswer(invocation -> {
                SingleRowResponse resp = invocation.getArgument(0);

                if (existed) {
                    Row row = new Row(SCHEMA, resp.getValue());

                    assertEquals(i, row.intValue(0));
                    assertEquals(i + 1, row.intValue(1));
                }
                else
                    assertNull(resp.getValue());

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void getAndDeleteValues(boolean existed) {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new GetAndDeleteCommand(getTestKey(i)));

            doAnswer(invocation -> {
                SingleRowResponse resp = invocation.getArgument(0);

                if (existed) {

                    Row row = new Row(SCHEMA, resp.getValue());

                    assertEquals(i, row.intValue(0));
                    assertEquals(i + 1, row.intValue(1));
                }
                else
                    assertNull(resp.getValue());

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void putIfExistValues(boolean existed) {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new ReplaceIfExistCommand(getTestRow(i, i + 1)));

            doAnswer(invocation -> {
                boolean result = invocation.getArgument(0);

                assertEquals(existed, result);

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * @param existed True if rows are existed, false otherwise.
     */
    private void deleteExactValues(boolean existed) {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new DeleteExactCommand(getTestRow(i, i + 1)));

            doAnswer(invocation -> {
                boolean result = invocation.getArgument(0);

                assertEquals(existed, result);

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * Replaces rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void replaceValues(boolean existed) {
        commandListener.onWrite(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new ReplaceCommand(getTestRow(i, i), getTestRow(i, i + 1)));

            doAnswer(invocation -> {
                assertTrue(invocation.getArgument(0) instanceof Boolean);

                boolean result = invocation.getArgument(0);

                assertEquals(existed, result);

                return null;
            }).when(clo).result(any());
        }));
    }

    /**
     * Prepares a test row which contains only key field.
     *
     * @return Row.
     */
    @NotNull private Row getTestKey(int key) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendInt(key);

        return new Row(SCHEMA, rowBuilder.build());
    }

    /**
     * Prepares a test row which contains key and value fields.
     *
     * @return Row.
     */
    @NotNull private Row getTestRow(int key, int val) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendInt(key);
        rowBuilder.appendInt(val);

        return new Row(SCHEMA, rowBuilder.build());
    }
}
