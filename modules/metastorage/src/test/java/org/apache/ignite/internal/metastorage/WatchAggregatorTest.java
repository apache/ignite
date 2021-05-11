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
import java.util.Collections;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WatchAggregatorTest {

    @Test
    public void testEventsRouting() {
        var watchAggregator = new WatchAggregator();
        var lsnr1 = mock(WatchListener.class);
        var lsnr2 = mock(WatchListener.class);
        watchAggregator.add(new Key("1"), lsnr1);
        watchAggregator.add(new Key("2"), lsnr2);

        var watchEvent1 = new WatchEvent(
            entry("1", "value1", 1, 1),
            entry("1", "value1n", 1, 1));
        var watchEvent2 = new WatchEvent(
            entry("2", "value2", 1, 1),
            entry("2", "value2n", 1, 1));
        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(Arrays.asList(watchEvent1, watchEvent2));

        verify(lsnr1).onUpdate(Collections.singletonList(watchEvent1));
        verify(lsnr2).onUpdate(Collections.singletonList(watchEvent2));
    }

    @Test
    public void testCancel() {
        var watchAggregator = new WatchAggregator();
        var lsnr1 = mock(WatchListener.class);
        when(lsnr1.onUpdate(any())).thenReturn(true);
        var lsnr2 = mock(WatchListener.class);
        when(lsnr2.onUpdate(any())).thenReturn(true);
        var id1 = watchAggregator.add(new Key("1"), lsnr1);
        var id2 = watchAggregator.add(new Key("2"), lsnr2);

        var watchEvent1 = new WatchEvent(
            entry("1", "value1", 1, 1),
            entry("1", "value1n", 1, 1));
        var watchEvent2 = new WatchEvent(
            entry("2", "value2", 1, 1),
            entry("2", "value2n", 1, 1));
        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(Arrays.asList(watchEvent1, watchEvent2));

        verify(lsnr1, times(1)).onUpdate(any());
        verify(lsnr2, times(1)).onUpdate(any());

        watchAggregator.cancel(id1);
        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(Arrays.asList(watchEvent1, watchEvent2));

        verify(lsnr1, times(1)).onUpdate(any());
        verify(lsnr2, times(2)).onUpdate(any());
    }

    @Test
    public void testCancelByFalseFromListener() {
        var watchAggregator = new WatchAggregator();
        var lsnr1 = mock(WatchListener.class);
        when(lsnr1.onUpdate(any())).thenReturn(false);
        var lsnr2 = mock(WatchListener.class);
        when(lsnr2.onUpdate(any())).thenReturn(true);
        var id1 = watchAggregator.add(new Key("1"), lsnr1);
        var id2 = watchAggregator.add(new Key("2"), lsnr2);

        var watchEvent1 = new WatchEvent(
            entry("1", "value1", 1, 1),
            entry("1", "value1n", 1, 1));
        var watchEvent2 = new WatchEvent(
            entry("2", "value2", 1, 1),
            entry("2", "value2n", 1, 1));
        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(Arrays.asList(watchEvent1, watchEvent2));

        verify(lsnr1, times(1)).onUpdate(any());
        verify(lsnr2, times(1)).onUpdate(any());

        watchAggregator.watch(1, (v1, v2) -> {}).get().listener().onUpdate(Arrays.asList(watchEvent1, watchEvent2));

        verify(lsnr1, times(1)).onUpdate(any());
        verify(lsnr2, times(2)).onUpdate(any());

    }

    private Entry entry(String key, String value, long revision, long updateCntr) {
        return new Entry() {
            @Override public @NotNull Key key() {
                return new Key(key);
            }

            @Override public @Nullable byte[] value() {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override public long revision() {
                return revision;
            }

            @Override public long updateCounter() {
                return updateCntr;
            }
        };
    }
}
