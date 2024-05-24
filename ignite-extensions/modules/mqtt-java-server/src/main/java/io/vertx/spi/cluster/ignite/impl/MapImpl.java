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

import org.apache.ignite.IgniteCache;

import javax.cache.Cache;
import java.util.*;

import static io.vertx.spi.cluster.ignite.impl.ClusterSerializationUtils.marshal;
import static io.vertx.spi.cluster.ignite.impl.ClusterSerializationUtils.unmarshal;

/**
 * Represents Apache Ignite cache as {@link java.util.Map} interface implementation.
 *
 * @author Andrey Gura
 */
public class MapImpl<K, V> implements Map<K, V> {

  private final IgniteCache<K, V> cache;

  /**
   * Constructor.
   *
   * @param cache Ignite cache instance.
   */
  public MapImpl(IgniteCache<K, V> cache) {
    this.cache = cache;
  }

  IgniteCache<K, V> getCache() {
    return cache;
  }

  @Override
  public int size() {
    return cache.size();
  }

  @Override
  public boolean isEmpty() {
    return cache.size() == 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean containsKey(Object key) {
    return cache.containsKey((K) marshal(key));
  }

  @Override
  public boolean containsValue(Object value) {
    for (Cache.Entry<K, V> entry : cache) {
      V v = unmarshal(entry.getValue());

      if (v.equals(value))
        return true;
    }

    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V get(Object key) {
    return unmarshal(cache.get((K) marshal(key)));
  }

  @Override
  public V put(K key, V value) {
    return unmarshal(cache.getAndPut(marshal(key), marshal(value)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public V remove(Object key) {
    return unmarshal(cache.getAndRemove((K) marshal(key)));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    Map<K, V> map0 = new HashMap<>();

    for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
      map0.put(marshal(entry.getKey()), marshal(entry.getValue()));
    }

    cache.putAll(map0);
  }

  @Override
  public void clear() {
    cache.clear();
  }

  @Override
  public Set<K> keySet() {
    Set<K> res = new HashSet<>();

    for (Cache.Entry<K, V> entry : cache) {
      res.add(unmarshal(entry.getKey()));
    }

    return res;
  }

  @Override
  public Collection<V> values() {
    Collection<V> res = new ArrayList<>();

    for (Cache.Entry<K, V> entry : cache) {
      res.add(unmarshal(entry.getValue()));
    }

    return res;
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> res = new HashSet<>();
    for (Cache.Entry<K, V> entry : cache) {
      res.add(new AbstractMap.SimpleImmutableEntry<>(unmarshal(entry.getKey()), unmarshal(entry.getValue())));
    }
    return res;
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return unmarshal(cache.getAndPutIfAbsent(marshal(key), marshal(value)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object key, Object value) {
    return cache.remove((K) marshal(key), (V) marshal(value));
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return cache.replace(marshal(key), marshal(oldValue), marshal(newValue));
  }

  @Override
  public V replace(K key, V value) {
    return unmarshal(cache.getAndReplace(marshal(key), marshal(value)));
  }
}
