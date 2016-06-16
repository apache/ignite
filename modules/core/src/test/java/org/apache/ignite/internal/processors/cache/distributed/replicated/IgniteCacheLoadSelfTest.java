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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests getOrCreateCache of cache that is being filled by loadCache.
 */
public class IgniteCacheLoadSelfTest extends GridCommonAbstractTest {
	/**
	 * Entry count.
	 */
	public static final int CNT = 1_500_000;

	/** */
	public static final String STATIC_CACHE_NAME = "static";

	/**
	 * {@inheritDoc}
	 */
	@Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
		IgniteConfiguration cfg = super.getConfiguration(gridName);

		CacheConfiguration ccfg = createCacheConfiguration(STATIC_CACHE_NAME);

		cfg.setCacheConfiguration(ccfg);

		return cfg;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override protected void afterTest() throws Exception {
		stopAllGrids();
	}


	/**
	 * @throws Exception if failed.
	 */
	public void testDynamicCache_3_nodes() throws Exception {
		IgniteEx ignite0 = startGrid(0);
		new Thread(new Runnable() {
			@Override public void run() {
				try {
					main( ignite0);
				} catch (InterruptedException | IOException | IgniteCheckedException e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
		Thread.sleep( 4000);
		IgniteEx ignite1 = startGrid(1);
		main(ignite1);
	}

	private CacheConfiguration<Object, Object> createCacheConfiguration(String cacheName) {
		CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName);
		ccfg.setCacheMode(CacheMode.REPLICATED);
		ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
		ccfg.setCacheStoreFactory(new TestFactory());
		return ccfg;
	}


	public void main(Ignite ignite) throws InterruptedException, IOException, IgniteCheckedException {

		final IgniteCache<Integer, String> testCache = ignite.getOrCreateCache(cacheConfig("testCache"));
		populateOrCheck(testCache);

		populateOrCheck(ignite.getOrCreateCache(cacheConfig("testCache2")));
		populateOrCheck(ignite.getOrCreateCache(cacheConfig("testCache3")));
	}

	private static void populateOrCheck(IgniteCache<Integer, String> testCache) throws InterruptedException {
		if (testCache.localSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP) == 0) {
			System.out.print("Cache is empty, loading...");
			//	fillCacheWithStreamer( testCache); // this always works
			fillWithLoadCache(testCache);   // this sometimes works, sometimes currentSize gets stuck
			System.out.println("Data loaded.");
		} else {

			int attempts = 0;
			int currentSize;
			while ((currentSize = testCache.localSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP)) < CNT) {
				System.out.println("currentSize = " + currentSize + " waiting for cache to get proper size");
				Thread.sleep(1000);
				if (attempts++ > 50) fail("Cache never reached expected size");
			}

			for (int i = 0; i < CNT; i++){
				final String s = testCache.get(i);
				if (!s.equals( genString(i))){
					throw new IllegalStateException("No good state");
				}

			}
			System.out.println("We seem to get correct data");
		}
	}

	private static void fillWithLoadCache(IgniteCache<Integer, String> testCache) {
		testCache.loadCache(new IgniteBiPredicate<Integer, String>() {
			@Override public boolean apply(Integer integer, String s) {
				return true;
			}
		});
	}

	private static void fillCacheWithStreamer(Ignite ignite, IgniteCache cache) {
		try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(cache.getName())) {
			streamer.allowOverwrite(true);

			for (int i = 0; i < CNT; i++) {
				streamer.addData(i, genString(i));
			}
		}
	}

	private static String genString(int i) {
		StringBuilder sb = new StringBuilder();
		sb.append("_");
		sb.append(Integer.toString(i));
		return sb.toString();
	}

	public static CacheConfiguration<Integer, String> cacheConfig(String testCache) {
		CacheConfiguration<Integer, String>  config = new CacheConfiguration<>();
		config.setName(testCache);
		config.setCacheMode(CacheMode.REPLICATED);
		config.setRebalanceMode(CacheRebalanceMode.SYNC);
		config.setCacheStoreFactory(new TestFactory());
		return config;
	}

	public static IgniteConfiguration simpleConfig(String gridName) throws IOException {
		IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
		igniteConfiguration.setFailureDetectionTimeout(30_000);
		igniteConfiguration.setWorkDirectory(java.nio.file.Files.createTempDirectory("tc").toFile().getAbsolutePath());
		igniteConfiguration.setGridName(gridName);
		igniteConfiguration.setSegmentationPolicy(SegmentationPolicy.NOOP);
		igniteConfiguration.setMetricsLogFrequency(0);
		return igniteConfiguration;
	}

	public static class TestFactory implements Factory<CacheStore<? super Object, ? super Object>>, Serializable {
		@Override public CacheStore<? super Object, ? super Object> create() {
			return new TestCacheStore();
		}
	}

	public static class TestCacheStore<Integer, String> implements CacheStore<Integer, String>, Serializable {

		@Override public void loadCache(IgniteBiInClosure clo, Object... args) throws CacheLoaderException {
			for (int i = 0; i < CNT; i++){
				clo.apply(i, genString(i));
			}
		}

		@Override public void sessionEnd(boolean commit) throws CacheWriterException {

		}

		@Override public Object load(Object o) throws CacheLoaderException {
			return null;
		}

		@Override public Map loadAll(Iterable iterable) throws CacheLoaderException {
			return null;
		}

		@Override public void write(Cache.Entry entry) throws CacheWriterException {

		}

		@Override public void writeAll(Collection collection) throws CacheWriterException {

		}

		@Override public void delete(Object o) throws CacheWriterException {

		}

		@Override public void deleteAll(Collection collection) throws CacheWriterException {

		}
	}
}
