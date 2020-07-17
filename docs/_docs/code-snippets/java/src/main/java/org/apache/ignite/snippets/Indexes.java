package org.apache.ignite.snippets;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

public class Indexes {

    // tag::configuring-with-annotation[]
    public class Person implements Serializable {
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
    // end::configuring-with-annotation[]

    public class Person2 implements Serializable {
        // tag::annotation-with-inline-size[]
        @QuerySqlField(index = true, inlineSize = 13)
        private String country;
        // end::annotation-with-inline-size[]
    }

    void register() {
        // tag::register-indexed-types[]
        // Preparing configuration.
        CacheConfiguration<Long, Person> ccfg = new CacheConfiguration<>();

        // Registering indexed type.
        ccfg.setIndexedTypes(Long.class, Person.class);
        // end::register-indexed-types[]
    }

    void executeQuery() {
        // tag::query[]
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT id, name FROM Person" + "WHERE id > 1500 LIMIT 10");
        // end::query[]
    }

    void withQueryEntities() {
        // tag::index-using-queryentity[]
        CacheConfiguration<Long, Person> cache = new CacheConfiguration<Long, Person>("myCache");

        QueryEntity queryEntity = new QueryEntity();

        queryEntity.setKeyFieldName("id").setKeyType(Long.class.getName()).setValueType(Person.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Long");
        fields.put("name", "java.lang.String");
        fields.put("salary", "java.lang.Long");

        queryEntity.setFields(fields);

        queryEntity.setIndexes(Arrays.asList(new QueryIndex("name"),
                new QueryIndex(Arrays.asList("id", "salary"), QueryIndexType.SORTED)));

        cache.setQueryEntities(Arrays.asList(queryEntity));

        // end::index-using-queryentity[]
    }

    void inline() {

        QueryEntity queryEntity = new QueryEntity();
        // tag::query-entity-with-inline-size[]
        QueryIndex idx = new QueryIndex("country");
        idx.setInlineSize(13);
        queryEntity.setIndexes(Arrays.asList(idx));
        // end::query-entity-with-inline-size[]
    }

    void customKeys() {
        Ignite ignite = Ignition.start();
        // tag::custom-key[]
        // Preparing cache configuration.
        CacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<Long, Person>("personCache");

        // Creating the query entity.
        QueryEntity entity = new QueryEntity("CustomKey", "Person");

        // Listing all the queryable fields.
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("intKeyField", Integer.class.getName());
        fields.put("strKeyField", String.class.getName());

        fields.put("firstName", String.class.getName());
        fields.put("lastName", String.class.getName());

        entity.setFields(fields);

        // Listing a subset of the fields that belong to the key.
        Set<String> keyFlds = new HashSet<>();

        keyFlds.add("intKeyField");
        keyFlds.add("strKeyField");

        entity.setKeyFields(keyFlds);

        // End of new settings, nothing else here is DML related

        entity.setIndexes(Collections.<QueryIndex>emptyList());

        cacheCfg.setQueryEntities(Collections.singletonList(entity));

        ignite.createCache(cacheCfg);

        // end::custom-key[]

    }

    public static void main(String[] args) {
        Indexes ind = new Indexes();

        ind.withQueryEntities();
    }
}
