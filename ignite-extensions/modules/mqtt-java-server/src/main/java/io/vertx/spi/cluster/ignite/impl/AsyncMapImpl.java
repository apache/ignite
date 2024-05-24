/*
 * Copyright (c) 2015 The original author or authors
 * ---------------------------------
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.spi.cluster.ignite.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteFuture;

import javax.cache.Cache;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.vertx.spi.cluster.ignite.impl.ClusterSerializationUtils.marshal;
import static io.vertx.spi.cluster.ignite.impl.ClusterSerializationUtils.unmarshal;

/**
 * Async wrapper for {@link MapImpl}.
 *
 * @author Andrey Gura
 */
public class AsyncMapImpl<K, V> implements AsyncMap<K, V> {

  private final VertxInternal vertx;
  private final IgniteCache<K, V> cache;

  /**
   * Constructor.
   *
   * @param cache {@link IgniteCache} instance.
   * @param vertx {@link Vertx} instance.
   */
  public AsyncMapImpl(IgniteCache<K, V> cache, VertxInternal vertx) {
    this.cache = cache;
    this.vertx = vertx;
  }

  @Override
  public Future<V> get(K k) {
    return execute(cache -> cache.getAsync(marshal(k)));
  }

  @Override
  public Future<Void> put(K k, V v) {
    return execute(cache -> cache.putAsync(marshal(k), marshal(v)));
  }

  @Override
  public Future<Void> put(K k, V v, long ttl) {
    return executeWithTtl(cache -> cache.putAsync(marshal(k), marshal(v)), ttl);
  }

  @Override
  public Future<V> putIfAbsent(K k, V v) {
    return execute(cache -> cache.getAndPutIfAbsentAsync(marshal(k), marshal(v)));
  }

  @Override
  public Future<V> putIfAbsent(K k, V v, long ttl) {
    return executeWithTtl(cache -> cache.getAndPutIfAbsentAsync(marshal(k), marshal(v)), ttl);
  }

  @Override
  public Future<V> remove(K k) {
    return execute(cache -> cache.getAndRemoveAsync(marshal(k)));
  }

  @Override
  public Future<Boolean> removeIfPresent(K k, V v) {
    return execute(cache -> cache.removeAsync(marshal(k), marshal(v)));
  }

  @Override
  public Future<V> replace(K k, V v) {
    return execute(cache -> cache.getAndReplaceAsync(marshal(k), marshal(v)));
  }

  @Override
  public Future<V> replace(K k, V v, long ttl) {
    return executeWithTtl(cache -> cache.getAndReplaceAsync(marshal(k), marshal(v)), ttl);
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
    return execute(cache -> cache.replaceAsync(marshal(k), marshal(oldValue), marshal(newValue)));
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue, long ttl) {
    return executeWithTtl(cache -> cache.replaceAsync(marshal(k), marshal(oldValue), marshal(newValue)), ttl);
  }

  @Override
  public Future<Void> clear() {
    return execute(IgniteCache::clearAsync);
  }

  @Override
  public Future<Integer> size() {
    return execute(IgniteCache::sizeAsync);
  }

  @Override
  public Future<Set<K>> keys() {
    return entries().map(Map::keySet);
  }

  @Override
  public Future<List<V>> values() {
    return entries().map(map -> new ArrayList<>(map.values()));
  }

  @Override
  public Future<Map<K, V>> entries() {
    return vertx.executeBlocking(
      promise -> {
        try {
          List<Cache.Entry<K, V>> all = cache.query(new ScanQuery<K, V>()).getAll();
          Map<K, V> map = new HashMap<>(all.size());
          for (Cache.Entry<K, V> entry : all) {
            map.put(unmarshal(entry.getKey()), unmarshal(entry.getValue()));
          }
          promise.complete(map);
        } catch (final RuntimeException cause) {
          promise.fail(new VertxException(cause));
        }
      }
    );
  }

  private <T> Future<T> execute(Function<IgniteCache<K, V>, IgniteFuture<T>> cacheOp) {
    return executeWithTtl(cacheOp, -1);
  }

  /**
   * @param ttl Time to live in ms.
   */
  private <T> Future<T> executeWithTtl(Function<IgniteCache<K, V>, IgniteFuture<T>> cacheOp, long ttl) {
    IgniteCache<K, V> cache0 = ttl > 0
      ? cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttl)))
      : cache;

    return vertx.executeBlocking(
      promise -> {
        IgniteFuture<T> future = cacheOp.apply(cache0);
        future.listen(
          fut -> {
            try {
              promise.complete(unmarshal(future.get()));
            } catch (final RuntimeException e) {
              promise.fail(new VertxException(e));
            }
          }
        );
      }
    );
  }
}
