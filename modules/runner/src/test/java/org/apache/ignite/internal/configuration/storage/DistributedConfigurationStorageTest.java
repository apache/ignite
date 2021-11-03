/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.configuration.storage;

import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Condition;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Tests for the {@link DistributedConfigurationStorage}.
 */
public class DistributedConfigurationStorageTest extends ConfigurationStorageTest {
    /**
     *
     */
    private final VaultManager vaultManager = new VaultManager(new InMemoryVaultService());

    /**
     *
     */
    private final KeyValueStorage metaStorage = new SimpleInMemoryKeyValueStorage();

    /**
     *
     */
    private final MetaStorageManager metaStorageManager = mockMetaStorageManager();

    /**
     *
     */
    @BeforeEach
    void start() {
        vaultManager.start();
        metaStorage.start();
        metaStorageManager.start();
    }

    /**
     *
     */
    @AfterEach
    void stop() throws Exception {
        metaStorageManager.stop();
        metaStorage.close();
        vaultManager.stop();
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationStorage getStorage() {
        return new DistributedConfigurationStorage(metaStorageManager, vaultManager);
    }

    /**
     * Creates a mock implementation of a {@link MetaStorageManager}.
     */
    private MetaStorageManager mockMetaStorageManager() {
        var mock = mock(MetaStorageManager.class);

        when(mock.invoke(any(), anyCollection(), anyCollection())).thenAnswer(invocation -> {
            Condition condition = invocation.getArgument(0);
            Collection<Operation> success = invocation.getArgument(1);
            Collection<Operation> failure = invocation.getArgument(2);

            boolean invokeResult = metaStorage.invoke(
                    toServerCondition(condition),
                    success.stream().map(DistributedConfigurationStorageTest::toServerOperation).collect(toList()),
                    failure.stream().map(DistributedConfigurationStorageTest::toServerOperation).collect(toList())
            );

            return CompletableFuture.completedFuture(invokeResult);
        });

        try {
            when(mock.range(any(), any())).thenAnswer(invocation -> {
                ByteArray keyFrom = invocation.getArgument(0);
                ByteArray keyTo = invocation.getArgument(1);

                return new CursorAdapter(metaStorage.range(keyFrom.bytes(), keyTo == null ? null : keyTo.bytes()));
            });
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }

        return mock;
    }

    /**
     * Converts a {@link Condition} to a {@link org.apache.ignite.internal.metastorage.server.Condition}.
     */
    private static org.apache.ignite.internal.metastorage.server.Condition toServerCondition(Condition condition) {
        switch (condition.type()) {
            case REV_LESS_OR_EQUAL:
                return new RevisionCondition(
                        RevisionCondition.Type.LESS_OR_EQUAL,
                        condition.inner().key(),
                        ((Condition.RevisionCondition) condition.inner()).revision()
                );
            case KEY_NOT_EXISTS:
                return new ExistenceCondition(
                        ExistenceCondition.Type.NOT_EXISTS,
                        condition.inner().key()
                );
            default:
                throw new UnsupportedOperationException("Unsupported condition type: " + condition.type());
        }
    }

    /**
     * Converts a {@link Operation} to a {@link org.apache.ignite.internal.metastorage.server.Operation}.
     */
    private static org.apache.ignite.internal.metastorage.server.Operation toServerOperation(Operation operation) {
        switch (operation.type()) {
            case PUT:
                return new org.apache.ignite.internal.metastorage.server.Operation(
                        OperationType.PUT,
                        operation.inner().key(),
                        ((Operation.PutOp) (operation.inner())).value()
                );
            case REMOVE:
                return new org.apache.ignite.internal.metastorage.server.Operation(
                        OperationType.REMOVE,
                        operation.inner().key(),
                        null
                );
            case NO_OP:
                return new org.apache.ignite.internal.metastorage.server.Operation(
                        OperationType.NO_OP,
                        null,
                        null
                );
            default:
                throw new UnsupportedOperationException("Unsupported operation type: " + operation.type());
        }
    }

    /**
     * {@code Cursor} that converts {@link Entry} to {@link org.apache.ignite.internal.metastorage.server.Entry}.
     */
    private static class CursorAdapter implements Cursor<Entry> {
        /** Internal cursor. */
        private final Cursor<org.apache.ignite.internal.metastorage.server.Entry> internalCursor;

        /**
         *
         */
        CursorAdapter(Cursor<org.apache.ignite.internal.metastorage.server.Entry> internalCursor) {
            this.internalCursor = internalCursor;
        }

        /** {@inheritDoc} */
        @Override
        public void close() throws Exception {
            internalCursor.close();
        }

        /** {@inheritDoc} */
        @NotNull
        @Override
        public Iterator<Entry> iterator() {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            return internalCursor.hasNext();
        }

        /** {@inheritDoc} */
        @Override
        public Entry next() {
            org.apache.ignite.internal.metastorage.server.Entry next = internalCursor.next();

            return new Entry() {
                @Override
                public @NotNull ByteArray key() {
                    return new ByteArray(next.key());
                }

                @Override
                public byte @Nullable [] value() {
                    return next.value();
                }

                @Override
                public long revision() {
                    return next.revision();
                }

                @Override
                public long updateCounter() {
                    return next.updateCounter();
                }

                @Override
                public boolean empty() {
                    return next.empty();
                }

                @Override
                public boolean tombstone() {
                    return next.tombstone();
                }
            };
        }
    }
}
