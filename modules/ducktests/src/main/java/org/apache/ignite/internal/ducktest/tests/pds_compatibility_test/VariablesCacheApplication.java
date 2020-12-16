//package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;
//
//import org.apache.ignite.IgniteCache;
//import org.apache.ignite.IgniteDataStreamer;
//import org.apache.ignite.cache.CacheAtomicityMode;
//import org.apache.ignite.cache.CacheMode;
//import org.apache.ignite.configuration.CacheConfiguration;
//import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
//import com.fasterxml.jackson.databind.JsonNode;
//import org.apache.ignite.compatibility.persistence.PersistenceBasicCompatibilityTest.*;
//
//
//import java.io.*;
//import java.util.UUID;
//
//
//public class VariablesCacheApplication extends IgniteAwareApplication {
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    protected void run(JsonNode jsonNode) {
//        log.info("Creating cache...");
//
//        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();
//        cacheCfg.setName(jsonNode.get("cacheName").asText())
//                .setCacheMode(CacheMode.PARTITIONED)
//                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
//
//        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheCfg);
//
//            cache.put(1, "data");
//            cache.put("1", "2");
//            cache.put(12, 2);
//            cache.put(13L, 2L);
//            cache.put(TestEnum.A, "Enum_As_Key");
//            cache.put("Enum_As_Value", TestEnum.B);
//            cache.put(TestEnum.C, TestEnum.C);
//            cache.put("Serializable", new TestSerializable(42));
//            cache.put(new TestSerializable(42), "Serializable_As_Key");
//            cache.put("Externalizable", new TestExternalizable(42));
//            cache.put(new TestExternalizable(42), "Externalizable_As_Key");
//            cache.put("testStringContainer", new TestStringContainerToBePrinted("testStringContainer"));
//            cache.put(UUID.fromString("DA9E0049-468C-4680-BF85-D5379164FDCC"), UUID.fromString("B851B870-3BA7-4E5F-BDB8-458B42300000"));
//
//        log.info("Cache created");
//        markSyncExecutionComplete();
//    }
//
//}
