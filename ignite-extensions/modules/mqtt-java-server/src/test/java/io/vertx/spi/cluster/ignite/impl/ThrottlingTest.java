/*
 * Copyright 2021 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.spi.cluster.ignite.impl;

import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

public class ThrottlingTest extends VertxTestBase {

  int threadCount = 4;
  ExecutorService executorService;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    executorService = Executors.newFixedThreadPool(threadCount);
  }

  @Test
  public void testInterval() throws Exception {
    int duration = 5;
    String[] addresses = {"foo", "bar", "baz", "qux"};

    ConcurrentMap<String, List<Long>> events = new ConcurrentHashMap<>(addresses.length);
    Throttling throttling = new Throttling((VertxInternal) vertx, address -> {
      events.compute(address, (k, v) -> {
        if (v == null) {
          v = Collections.synchronizedList(new LinkedList<>());
        }
        v.add(System.nanoTime());
        return v;
      });
      Promise<Void> promise = Promise.promise();
      vertx.setTimer(1, l -> promise.complete());
      return promise.future();
    });

    CountDownLatch latch = new CountDownLatch(threadCount);
    long start = System.nanoTime();
    for (int i = 0; i < threadCount; i++) {
      executorService.submit(() -> {
        try {
          do {
            sleepMax(5);
            throttling.onEvent(addresses[ThreadLocalRandom.current().nextInt(addresses.length)]);
          } while (SECONDS.convert(System.nanoTime() - start, NANOSECONDS) < duration);
        } finally {
          latch.countDown();
        }
      });
    }
    latch.await();

    assertWaitUntil(() -> {
      if (events.size() != addresses.length) {
        return false;
      }
      for (List<Long> nanoTimes : events.values()) {
        Long previous = null;
        for (Long nanoTime : nanoTimes) {
          if (previous != null) {
            if (MILLISECONDS.convert(nanoTime - previous, NANOSECONDS) < 20) {
              return false;
            }
          }
          previous = nanoTime;
        }
      }
      return true;
    }, 1000);
  }

  private void sleepMax(long time) {
    try {
      MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(time));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(5, SECONDS));
    super.tearDown();
  }
}
