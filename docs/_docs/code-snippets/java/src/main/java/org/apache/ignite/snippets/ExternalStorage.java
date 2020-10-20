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
package org.apache.ignite.snippets;

import com.mysql.cj.jdbc.MysqlDataSource;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.cache.store.jdbc.dialect.MySQLDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class ExternalStorage {

    public static void cacheJdbcPojoStoreExample() {
        //tag::pojo[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        CacheConfiguration<Integer, Person> personCacheCfg = new CacheConfiguration<>();

        personCacheCfg.setName("PersonCache");
        personCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        personCacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        personCacheCfg.setReadThrough(true);
        personCacheCfg.setWriteThrough(true);

        CacheJdbcPojoStoreFactory<Integer, Person> factory = new CacheJdbcPojoStoreFactory<>();
        factory.setDialect(new MySQLDialect());
        factory.setDataSourceFactory((Factory<DataSource>)() -> {
            MysqlDataSource mysqlDataSrc = new MysqlDataSource();
            mysqlDataSrc.setURL("jdbc:mysql://[host]:[port]/[database]");
            mysqlDataSrc.setUser("YOUR_USER_NAME");
            mysqlDataSrc.setPassword("YOUR_PASSWORD");
            return mysqlDataSrc;
        });

        JdbcType personType = new JdbcType();
        personType.setCacheName("PersonCache");
        personType.setKeyType(Integer.class);
        personType.setValueType(Person.class);
        // Specify the schema if applicable
        // personType.setDatabaseSchema("MY_DB_SCHEMA");
        personType.setDatabaseTable("PERSON");

        personType.setKeyFields(new JdbcTypeField(java.sql.Types.INTEGER, "id", Integer.class, "id"));

        personType.setValueFields(new JdbcTypeField(java.sql.Types.INTEGER, "id", Integer.class, "id"));
        personType.setValueFields(new JdbcTypeField(java.sql.Types.VARCHAR, "name", String.class, "name"));

        factory.setTypes(personType);

        personCacheCfg.setCacheStoreFactory(factory);

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType(Person.class.getName());
        qryEntity.setKeyFieldName("id");

        Set<String> keyFields = new HashSet<>();
        keyFields.add("id");
        qryEntity.setKeyFields(keyFields);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("name", "java.lang.String");

        qryEntity.setFields(fields);

        personCacheCfg.setQueryEntities(Collections.singletonList(qryEntity));

        igniteCfg.setCacheConfiguration(personCacheCfg);
        //end::pojo[]
    }

    //tag::person[]
    class Person implements Serializable {
        private static final long serialVersionUID = 0L;

        private int id;

        private String name;

        public Person() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }
    //end::person[]

    public static void cacheJdbcBlobStoreExample() {
        //tag::blob1[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        CacheConfiguration<Integer, Person> personCacheCfg = new CacheConfiguration<>();
        personCacheCfg.setName("PersonCache");

        CacheJdbcBlobStoreFactory<Integer, Person> cacheStoreFactory = new CacheJdbcBlobStoreFactory<>();

        cacheStoreFactory.setUser("USER_NAME");

        MysqlDataSource mysqlDataSrc = new MysqlDataSource();
        mysqlDataSrc.setURL("jdbc:mysql://[host]:[port]/[database]");
        mysqlDataSrc.setUser("USER_NAME");
        mysqlDataSrc.setPassword("PASSWORD");

        cacheStoreFactory.setDataSource(mysqlDataSrc);

        personCacheCfg.setCacheStoreFactory(cacheStoreFactory);

        personCacheCfg.setWriteThrough(true);
        personCacheCfg.setReadThrough(true);

        igniteCfg.setCacheConfiguration(personCacheCfg);
        //end::blob1[]

        Ignite ignite = Ignition.start(igniteCfg);

        //tag::blob2[]
        // Load data from person table into PersonCache.
        IgniteCache<Integer, Person> personCache = ignite.cache("PersonCache");

        personCache.loadCache(null);
        //end::blob2[]
    }
}
