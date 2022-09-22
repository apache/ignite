package de.kp.works.ignite;

/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import de.kp.works.janus.AbstractEntryBuilder;
import de.kp.works.janus.IgniteCacheEntry;
import de.kp.works.janus.IgniteKeyIterator;
import de.kp.works.janus.IgniteValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

public class IgniteClient extends AbstractEntryBuilder {

	private static final Log log = LogFactory.getLog(IgniteClient.class);

	private final Ignite ignite;

	public IgniteClient(Ignite ignite) {
		this.ignite = ignite;
	}

	/*
	 * This method retrieves a certain column slice (range key) and returns the
	 * respective row keys (hash keys);
	 * 
	 * hash & range keys are encoded as hex strings, i.e. lexical ordering is
	 * supported and will be used within this key query mechanism
	 * 
	 */
	public KeyIterator getKeySlice(IgniteCache<String, BinaryObject> cache, SliceQuery query,
			Map<String, IgniteValue> items) {

		Transaction tx = ignite.transactions().txStart();

		try {

			String cacheName = cache.getName();
			/*
			 * Retrieve columns (or range keys) in their hexadecimal [String] representation
			 */
			String rangeKeyStart = items.get(RANGE_KEY_START).getS();
			String rangeKeyEnd = items.get(RANGE_KEY_END).getS();
			/*
			 * Build the initial Apache Ignite SQL statement to specify the return fields
			 * and the associated cache
			 */
			String sql = "select HASH_KEY, RANGE_KEY, BYTE_BUFFER from " + cacheName;
			/*
			 * Check whether a single range key is provided with the respect
			 */
			if (rangeKeyStart.compareTo(rangeKeyEnd) >= 0) {
				/*
				 * This is a request that does not provide a range key slice but focuses on a
				 * single column value
				 */
				sql += " where RANGE_KEY = '" + rangeKeyStart + "'";

			} else {
				sql += " where RANGE_KEY >= '" + rangeKeyStart + "' and RANGE_KEY < '" + rangeKeyEnd + "'";

			}
			/*
			 * EXPERIENCE: JanusGraph offers a variety of features from ordered keys to
			 * ordered and unordered scans; but, even for unordered scans, and, limited to a
			 * single result, JanusGraph requires the largest row key
			 */
			sql += " order by HASH_KEY ASC";
			SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sql);

			/*
			 * The result contains all rows that the provided column values; note,
			 * JanusGraph leverages the column name as its value and the associated byte
			 * buffer is not relevant in this case
			 */
			List<List<?>> results = cache.query(sqlQuery).getAll();

			tx.commit();

			/* HASH_KEY (0), RANGE_KEY (1), BYTE_BUFFER (2) */

			List<Entry> entries = results.stream().map(result -> {

				String rowKey = (String) result.get(0);
				StaticBuffer hashKey = decodeKeyFromHexString(rowKey);

				ByteBuffer byteBuffer = (ByteBuffer) result.get(2);
				StaticBuffer value = decodeValue(byteBuffer);

				if (value == null)
					return StaticArrayEntry.of(hashKey);

				else
					return StaticArrayEntry.of(hashKey, value);

			}).collect(Collectors.toList());

			return new IgniteKeyIterator(entries);

		} catch (Exception e) {

			log.error(String.format("[IgniteClient] getKeySlice failed with: %s", e.getLocalizedMessage()));

			tx.rollback();

			List<Entry> entries = Collections.emptyList();
			return new IgniteKeyIterator(entries);

		}

	}

	public List<Entry> getColumnRange(IgniteCache<String, BinaryObject> cache, KeySliceQuery query,
			Map<String, IgniteValue> items) {

		Transaction tx = ignite.transactions().txStart();

		try {

			String cacheName = cache.getName();
			String rowKey = items.get(HASH_KEY).getS();

			String rangeKeyStart = items.get(RANGE_KEY_START).getS();
			String rangeKeyEnd = items.get(RANGE_KEY_END).getS();

			String sql = "select HASH_KEY, RANGE_KEY, BYTE_BUFFER from " + cacheName + " WHERE HASH_KEY = '" + rowKey
					+ "'";
			sql += " and RANGE_KEY >= '" + rangeKeyStart + "' and RANGE_KEY < '" + rangeKeyEnd + "'";

			sql += " order by RANGE_KEY ASC";
			SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sql);
			/*
			 * The result contains all columns that refer to the provided row key
			 */
			List<List<?>> results = cache.query(sqlQuery).getAll();
			tx.commit();

			if (results.isEmpty())
				return Collections.emptyList();
			/*
			 * Extract columns and start
			 */
			return results.stream().map(result -> {

				String colName = (String) result.get(1);
				StaticBuffer rangeKey = decodeRangeKey(colName);

				ByteBuffer colValu = (ByteBuffer) result.get(2);
				StaticBuffer value = decodeValue(colValu);
				/*
				 * There are stores (e.g. janusgraph_ids) that use the column name also as its
				 * value; this implies that the value = null
				 */
				if (value == null)
					return StaticArrayEntry.of(rangeKey);

				else
					return StaticArrayEntry.of(rangeKey, value);

			}).collect(Collectors.toList());

		} catch (Exception e) {

			log.error(String.format("[IgniteClient] getColumnRange failed with: %s", e.getLocalizedMessage()));
			tx.rollback();

			return Collections.emptyList();

		}

	}

	public void put(IgniteCache<String, BinaryObject> cache, IgniteCacheEntry entry) {

		if (ignite == null)
			return;

		String cacheKey = entry.getCacheKey();
		cache.put(cacheKey, buildObject(cache.getName(), entry));

	}

	public void putAll(IgniteCache<String, BinaryObject> cache, List<IgniteCacheEntry> entries) {

		if (ignite == null)
			return;

		Transaction tx = ignite.transactions().txStart();

		try {

			for (IgniteCacheEntry entry : entries) {
				put(cache, entry);
			}

			tx.commit();

		} catch (Exception e) {

			log.error(String.format("[IgniteClient] putAll failed with: %s", e.getLocalizedMessage()));
			tx.rollback();

		}

	}

	public void removeAll(IgniteCache<String, BinaryObject> cache, List<IgniteCacheEntry> entries) {

		if (entries.isEmpty())
			return;

		Transaction tx = ignite.transactions().txStart();

		try {
			/*
			 * The entries specify a single hash key (row) and a list of range keys (column
			 * names); to delete the respective cache entries, we have to retrieve their
			 * keys and after that remove the entries
			 *
			 * The hash key is extracted from the first entry as is it always the same for
			 * all entries
			 *
			 */
			String hashKey = entries.get(0).getHashKey();

			List<String> rangeKeys = entries.stream()
					.map(entry -> "'" + entry.getRangeKey() + "'").collect(Collectors.toList());

			String inExpr = String.join(",", rangeKeys);
			String sql = "select _key from " + cache.getName() + " where HASH_KEY = '" + hashKey + "' and RANGE_KEY in ("
					+ inExpr + ")";

			SqlFieldsQuery query = new SqlFieldsQuery(sql);
			List<List<?>> results = cache.query(query).getAll();

			if (results.isEmpty())
				return;

			List<String> keys = results.stream()
					.map(items -> (String) items.get(0)).collect(Collectors.toList());

			cache.removeAll(new HashSet<>(keys));
			tx.commit();

		} catch (Exception e) {

			log.error(String.format("[IgniteClient] removeAll failed with: %s", e.getLocalizedMessage()));
			tx.rollback();

		}

	}

	private BinaryObject buildObject(String table, IgniteCacheEntry entry) {

		BinaryObjectBuilder valueBuilder = ignite.binary().builder(table);

		String hashKey = entry.getHashKey();
		valueBuilder.setField(HASH_KEY, hashKey);

		String rangeKey = entry.getRangeKey();
		valueBuilder.setField(RANGE_KEY, rangeKey);

		ByteBuffer buffer = entry.getBuffer();
		valueBuilder.setField(BYTE_BUFFER, buffer);

		return valueBuilder.build();

	}

	public IgniteCache<String, BinaryObject> getOrCreateCache(String name) {
		/*
		 * .withKeepBinary() must not be used here
		 */
		if (ignite == null)
			return null;
		boolean exists = ignite.cacheNames().contains(name);
		if (exists)
			return ignite.cache(name);

		else
			return createCache(name);

	}

	private IgniteCache<String, BinaryObject> createCache(String cacheName) {
		return createCache(cacheName, CacheMode.REPLICATED);
	}

	private IgniteCache<String, BinaryObject> createCache(String cacheName, CacheMode cacheMode) {

		CacheConfiguration<String, BinaryObject> cfg = createCacheCfg(cacheName, cacheMode);
		return ignite.createCache(cfg);// ;.withKeepBinary();

	}

	private CacheConfiguration<String, BinaryObject> createCacheCfg(String table, CacheMode cacheMode) {
		/*
		 * Defining query entities is the Apache Ignite mechanism to dynamically define
		 * a queryable 'class'
		 */
		QueryEntity qe = buildQueryEntity(table);

		List<QueryEntity> qes = new java.util.ArrayList<>();
		qes.add(qe);
		/*
		 * Specify Apache Ignite cache configuration; it is important to leverage
		 * 'BinaryObject' as well as 'setStoreKeepBinary'
		 */
		CacheConfiguration<String, BinaryObject> cfg = new CacheConfiguration<>();
		cfg.setName(table);

		cfg.setStoreKeepBinary(false);
		cfg.setIndexedTypes(String.class, BinaryObject.class);

		cfg.setCacheMode(cacheMode);

		cfg.setQueryEntities(qes);
		return cfg;

	}

	private QueryEntity buildQueryEntity(String table) {

		QueryEntity qe = new QueryEntity();
		/*
		 * The key type of the Apache Ignite cache is set to [String], i.e. an
		 * independent identity management is used here
		 */
		qe.setKeyType("java.lang.String");
		/*
		 * The 'table' is used as table name in select statement as well as the name of
		 * 'ValueType'
		 */
		qe.setValueType(table);
		/*
		 * Define fields for the Apache Ignite cache that is used as one of the data
		 * backends of JanusGraph
		 */
		LinkedHashMap<String, String> fields = new LinkedHashMap<>();
		/*
		 * The hash key used by JanusGraph to identify an equivalent of a data row
		 */
		fields.put("HASH_KEY", "java.lang.String");
		/*
		 * The range used by JanusGraph to identify an equivalent of a column name;
		 * however, the range key may also directly be used as the respective data value
		 */
		fields.put("RANGE_KEY", "java.lang.String");
		/*
		 * The [ByteBuffer] representation fo the column value that can be converted
		 * into JabusGraph's [StaticBuffer]
		 */
		fields.put("BYTE_BUFFER", "java.nio.ByteBuffer");

		qe.setFields(fields);
		return qe;

	}

}
