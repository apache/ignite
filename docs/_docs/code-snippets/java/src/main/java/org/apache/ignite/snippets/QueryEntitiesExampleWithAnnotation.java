package org.apache.ignite.snippets;

import java.io.Serializable;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

public class QueryEntitiesExampleWithAnnotation {
// tag::query-entity-annotation[]
	class Person implements Serializable {
		/** Indexed field. Will be visible to the SQL engine. */
		@QuerySqlField(index = true)
		private long id;

		/** Queryable field. Will be visible to the SQL engine. */
		@QuerySqlField
		private String name;

		/** Will NOT be visible to the SQL engine. */
		private int age;

		/**
		 * Indexed field sorted in descending order. Will be visible to the SQL engine.
		 */
		@QuerySqlField(index = true, descending = true)
		private float salary;
	}

	public static void main(String[] args) {
		Ignite ignite = Ignition.start();
		CacheConfiguration<Long, Person> personCacheCfg = new CacheConfiguration<Long, Person>();
		personCacheCfg.setName("Person");

		personCacheCfg.setIndexedTypes(Long.class, Person.class);
		IgniteCache<Long, Person> cache = ignite.createCache(personCacheCfg);
	}

// end::query-entity-annotation[]
}
