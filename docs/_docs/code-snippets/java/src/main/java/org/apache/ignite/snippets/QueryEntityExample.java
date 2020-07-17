package org.apache.ignite.snippets;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;


public class QueryEntityExample {
	// tag::query-entity[]
	class Person implements Serializable {
		private long id;

		private String name;

		private int age;

		private float salary;
	}

	public static void main(String[] args) {
		Ignite ignite = Ignition.start();
		CacheConfiguration<Long, Person> personCacheCfg = new CacheConfiguration<Long, Person>();
		personCacheCfg.setName("Person");

		QueryEntity queryEntity = new QueryEntity(Long.class, Person.class)
		        .addQueryField("id", Long.class.getName(), null).addQueryField("age", Integer.class.getName(), null)
		        .addQueryField("salary", Float.class.getName(), null)
		        .addQueryField("name", String.class.getName(), null);

		queryEntity.setIndexes(Arrays.asList(new QueryIndex("id"), new QueryIndex("salary", false)));

		personCacheCfg.setQueryEntities(Arrays.asList(queryEntity));

		IgniteCache<Long, Person> cache = ignite.createCache(personCacheCfg);
	}
	// end::query-entity[]
}
