/*
 * Copyright 2020 Red Hat, Inc.
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
package io.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryEnumCache;
import org.apache.ignite.internal.util.GridClassLoaderCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerExclusions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Thomas Segismont
 * @author Lukas Prettenthaler
 */
public class Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(Lifecycle.class);

  public static void close(List<Vertx> clustered) throws Exception {
    CountDownLatch latch = new CountDownLatch(clustered.size());
    for (Vertx clusteredVertx : clustered) {
      Thread.sleep(500L);
      clusteredVertx.close().onComplete(ar -> {
        if (ar.failed()) {
          log.error("Failed to shutdown vert.x", ar.cause());
        }
        latch.countDown();
      });
    }
    assertTrue(latch.await(180, TimeUnit.SECONDS));

    Thread.sleep(200L);

    Collection<Ignite> list = new ArrayList<>(G.allGrids());

    for (Ignite g : list) {
      stopGrid(g.name());
    }

    List<Ignite> nodes = G.allGrids();

    assert nodes.isEmpty() : nodes;

    GridClassLoaderCache.clear();
    U.clearClassCache();
    MarshallerExclusions.clearCache();
    BinaryEnumCache.clear();
  }

  private static void stopGrid(String igniteInstanceName) {
    try {
      IgniteEx ignite = (IgniteEx) G.ignite(igniteInstanceName);

      assert ignite != null : "Ignite returned null grid for name: " + igniteInstanceName;

      UUID id = ignite.context().localNodeId();
      log.info(">>> Stopping grid [name=" + ignite.name() + ", id=" + id + ']');
      IgniteUtils.setCurrentIgniteName(igniteInstanceName);

      try {
        G.stop(igniteInstanceName, true);
      } finally {
        IgniteUtils.setCurrentIgniteName(null);
      }
    } catch (IllegalStateException ignored) {
      // Ignore error if grid already stopped.
    } catch (Throwable e) {
      log.error("Failed to stop grid [igniteInstanceName=" + igniteInstanceName + ']', e);
    }
  }
}
