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

import io.vertx.core.Future;
import io.vertx.core.impl.VertxInternal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class Throttling {

  // @formatter:off
  private enum State {
    NEW {
      State pending() {
        return PENDING;
      }

      State start() {
        return RUNNING;
      }

      State done() {
        throw new IllegalStateException();
      }

      State next() {
        throw new IllegalStateException();
      }
    },
    PENDING {
      State pending() {
        return this;
      }

      State start() {
        return RUNNING;
      }

      State done() {
        throw new IllegalStateException();
      }

      State next() {
        throw new IllegalStateException();
      }
    },
    RUNNING {
      State pending() {
        return RUNNING_PENDING;
      }

      State start() {
        throw new IllegalStateException();
      }

      State done() {
        return FINISHED;
      }

      State next() {
        throw new IllegalStateException();
      }
    },
    RUNNING_PENDING {
      State pending() {
        return this;
      }

      State start() {
        throw new IllegalStateException();
      }

      State done() {
        return FINISHED_PENDING;
      }

      State next() {
        throw new IllegalStateException();
      }
    },
    FINISHED {
      State pending() {
        return FINISHED_PENDING;
      }

      State start() {
        throw new IllegalStateException();
      }

      State done() {
        throw new IllegalStateException();
      }

      State next() {
        return null;
      }
    },
    FINISHED_PENDING {
      State pending() {
        return this;
      }

      State start() {
        throw new IllegalStateException();
      }

      State done() {
        throw new IllegalStateException();
      }

      State next() {
        return NEW;
      }
    };

    abstract State pending();

    abstract State start();

    abstract State done();

    abstract State next();
  }
  // @formatter:on

  private final VertxInternal vertx;
  private final Function<String, Future<?>> action;
  private final ConcurrentMap<String, State> map;

  public Throttling(VertxInternal vertx, Function<String, Future<?>> action) {
    this.vertx = vertx;
    this.action = action;
    map = new ConcurrentHashMap<>();
  }

  public void onEvent(String address) {
    State curr = map.compute(address, (s, state) -> state == null ? State.NEW : state.pending());
    if (curr == State.NEW) {
      run(address);
    }
  }

  private void run(String address) {
    map.computeIfPresent(address, (s, state) -> state.start());
    action.apply(address).onComplete(ar -> {
      map.computeIfPresent(address, (s, state) -> state.done());
      vertx.setTimer(20, l -> {
        checkState(address);
      });
    });
  }

  private void checkState(String address) {
    State curr = map.computeIfPresent(address, (s, state) -> state.next());
    if (curr == State.NEW) {
      run(address);
    }
  }
}
