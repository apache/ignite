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

package org.apache.ignite.internal.metastorage;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.internal.metastorage.watch.KeyCriterion;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.metastorage.client.Entry;
import org.apache.ignite.metastorage.client.EntryEvent;
import org.apache.ignite.metastorage.client.WatchEvent;
import org.apache.ignite.metastorage.client.WatchListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class WatchAggregatorTest {
    /**
     *
     */
    @Test
    public void testEventsRouting() {
        var watchAggregator = new WatchAggregator();
        var lsnr1 = mock(WatchListener.class);
        var lsnr2 = mock(WatchListener.class);

        watchAggregator.add(new ByteArray("1"), lsnr1);
        watchAggregator.add(new ByteArray("2"), lsnr2);

        var entryEvt1 = new EntryEvent(
                entry("1", "value1", 1, 1),
                entry("1", "value1n", 1, 1)
        );

        var entryEvt2 = new EntryEvent(
                entry("2", "value2", 1, 1),
                entry("2", "value2n", 1, 1)
        );

        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(new WatchEvent(List.of(entryEvt1, entryEvt2)));

        var watchEvt1Res = ArgumentCaptor.forClass(WatchEvent.class);
        verify(lsnr1).onUpdate(watchEvt1Res.capture());
        assertEquals(List.of(entryEvt1), watchEvt1Res.getValue().entryEvents());

        var watchEvt2Res = ArgumentCaptor.forClass(WatchEvent.class);
        verify(lsnr2).onUpdate(watchEvt2Res.capture());
        assertEquals(List.of(entryEvt2), watchEvt2Res.getValue().entryEvents());
    }

    /**
     *
     */
    @Test
    public void testCancel() {
        var watchAggregator = new WatchAggregator();
        var lsnr1 = mock(WatchListener.class);
        when(lsnr1.onUpdate(any())).thenReturn(true);
        var lsnr2 = mock(WatchListener.class);
        when(lsnr2.onUpdate(any())).thenReturn(true);
        var id1 = watchAggregator.add(new ByteArray("1"), lsnr1);
        var id2 = watchAggregator.add(new ByteArray("2"), lsnr2);

        var entryEvt1 = new EntryEvent(
                entry("1", "value1", 1, 1),
                entry("1", "value1n", 1, 1)
        );

        var entryEvt2 = new EntryEvent(
                entry("2", "value2", 1, 1),
                entry("2", "value2n", 1, 1)
        );

        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(new WatchEvent(List.of(entryEvt1, entryEvt2)));

        verify(lsnr1, times(1)).onUpdate(any());
        verify(lsnr2, times(1)).onUpdate(any());

        watchAggregator.cancel(id1);
        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(new WatchEvent(List.of(entryEvt1, entryEvt2)));

        verify(lsnr1, times(1)).onUpdate(any());
        verify(lsnr2, times(2)).onUpdate(any());
    }

    /**
     *
     */
    @Test
    public void testCancelByFalseFromListener() {
        var watchAggregator = new WatchAggregator();
        var lsnr1 = mock(WatchListener.class);
        when(lsnr1.onUpdate(any())).thenReturn(false);
        var lsnr2 = mock(WatchListener.class);
        when(lsnr2.onUpdate(any())).thenReturn(true);
        var id1 = watchAggregator.add(new ByteArray("1"), lsnr1);
        var id2 = watchAggregator.add(new ByteArray("2"), lsnr2);

        var entryEvt1 = new EntryEvent(
            entry("1", "value1", 1, 1),
            entry("1", "value1n", 1, 1)
        );

        var entryEvt2 = new EntryEvent(
            entry("2", "value2", 1, 1),
            entry("2", "value2n", 1, 1)
        );

        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(new WatchEvent(List.of(entryEvt1, entryEvt2)));

        verify(lsnr1, times(1)).onUpdate(any());
        verify(lsnr2, times(1)).onUpdate(any());

        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(new WatchEvent(List.of(entryEvt1, entryEvt2)));

        verify(lsnr1, times(1)).onUpdate(any());
        verify(lsnr2, times(2)).onUpdate(any());

    }

    /**
     *
     */
    @Test
    public void testOneCriterionInference() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(new ByteArray("key"), null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        assertEquals(new KeyCriterion.ExactCriterion(new ByteArray("key")), keyCriterion);
    }

    /**
     *
     */
    @Test
    public void testTwoExactCriteriaUnion() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(new ByteArray("key1"), null);
        watchAggregator.add(new ByteArray("key2"), null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        var expKeyCriterion = new KeyCriterion.CollectionCriterion(
            new HashSet<>(Arrays.asList(new ByteArray("key1"), new ByteArray("key2")))
        );
        assertEquals(expKeyCriterion, keyCriterion);
    }

    /**
     *
     */
    @Test
    public void testTwoEqualExactCriteriaUnion() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(new ByteArray("key1"), null);
        watchAggregator.add(new ByteArray("key1"), null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        assertEquals(keyCriterion, keyCriterion);
    }

    /**
     *
     */
    @Test
    public void testThatKeyCriteriaUnionAssociative() {
        var data = Arrays.asList(
            new KeyCriterion.RangeCriterion(new ByteArray("0"), new ByteArray("5")),
            new KeyCriterion.CollectionCriterion(Arrays.asList(new ByteArray("1"), new ByteArray("2"))),
            new KeyCriterion.ExactCriterion(new ByteArray("3")));

        for (int i = 0; i < data.size() - 1; i++) {
            for (int j = i + 1; j < data.size(); j++)
                assertEquals(data.get(i).union(data.get(j)), data.get(j).union(data.get(i)));
        }
    }

    /**
     *
     */
    @Test
    public void testTwoEqualCollectionCriteriaUnion() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(Arrays.asList(new ByteArray("key1"), new ByteArray("key2")), null);
        watchAggregator.add(Arrays.asList(new ByteArray("key1"), new ByteArray("key2")), null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        var expKeyCriterion = new KeyCriterion.CollectionCriterion(
            new HashSet<>(Arrays.asList(new ByteArray("key1"), new ByteArray("key2")))
        );
        assertEquals(expKeyCriterion, keyCriterion);
    }

    /**
     *
     */
    @Test
    public void testExactInTheMiddleAndRangeCriteriaOnTheEdgesUnion() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(new ByteArray("key1"), null);
        watchAggregator.add(new ByteArray("key0"), new ByteArray("key2"), null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        var expKeyCriterion = new KeyCriterion.RangeCriterion(
            new ByteArray("key0"), new ByteArray("key2"));
        assertEquals(expKeyCriterion, keyCriterion);
    }

    /**
     *
     */
    @Test
    public void testHighExactAndLowerRangeCriteriaUnion() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(new ByteArray("key3"), null);
        watchAggregator.add(new ByteArray("key0"), new ByteArray("key2"), null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        var expKeyCriterion = new KeyCriterion.RangeCriterion(
            new ByteArray("key0"), new ByteArray("key4"));
        assertEquals(expKeyCriterion, keyCriterion);
    }

    /**
     *
     */
    @Test
    public void testNullKeyAsStartOfRangeCriterion() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(new ByteArray("key0"), null);
        watchAggregator.add(null, new ByteArray("key2"), null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        var expKeyCriterion = new KeyCriterion.RangeCriterion(
            null, new ByteArray("key2"));
        assertEquals(expKeyCriterion, keyCriterion);
    }

    /**
     *
     */
    @Test
    public void testNullKeyAsEndOfRangeCriterion() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(new ByteArray("key3"), null);
        watchAggregator.add(new ByteArray("key1"), null, null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        var expKeyCriterion = new KeyCriterion.RangeCriterion(
            new ByteArray("key1"), null);
        assertEquals(expKeyCriterion, keyCriterion);
    }

    /**
     *
     */
    @Test
    public void testAllTypesOfCriteriaUnion() {
        var watchAggregator = new WatchAggregator();

        watchAggregator.add(new ByteArray("key0"), null);
        watchAggregator.add(new ByteArray("key1"), new ByteArray("key2"), null);
        watchAggregator.add(Arrays.asList(new ByteArray("key0"), new ByteArray("key3")), null);

        var keyCriterion = watchAggregator.watch(0, null).get().keyCriterion();
        var expKeyCriterion = new KeyCriterion.RangeCriterion(
            new ByteArray("key0"), new ByteArray("key4"));
        assertEquals(expKeyCriterion, keyCriterion);
    }

    /**
     *
     */
    private Entry entry(String key, String val, long revision, long updateCntr) {
        return new Entry() {
            /** {@inheritDoc} */
            @Override public @NotNull ByteArray key() {
                return new ByteArray(key);
            }

            /** {@inheritDoc} */
            @Override public @Nullable byte[] value() {
                return val.getBytes(StandardCharsets.UTF_8);
            }

            /** {@inheritDoc} */
            @Override public long revision() {
                return revision;
            }

            /** {@inheritDoc} */
            @Override public long updateCounter() {
                return updateCntr;
            }

            /** {@inheritDoc} */
            @Override public boolean empty() {
                return false;
            }

            /** {@inheritDoc} */
            @Override public boolean tombstone() {
                return false;
            }
        };
    }
}
