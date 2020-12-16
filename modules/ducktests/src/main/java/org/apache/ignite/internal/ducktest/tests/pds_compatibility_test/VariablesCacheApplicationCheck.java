//package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;
//
//import org.apache.ignite.IgniteCache;
//import org.apache.ignite.cache.CacheAtomicityMode;
//import org.apache.ignite.cache.CacheMode;
//import org.apache.ignite.configuration.CacheConfiguration;
//import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
////import org.apache.ignite.internal.ducktest.tests.pds_compatibility_test.VariablesCacheApplication.*;
//import com.fasterxml.jackson.databind.JsonNode;
//
//import org.apache.ignite.compatibility.persistence.PersistenceBasicCompatibilityTest.*;
//
//import java.util.UUID;
//
//public class VariablesCacheApplicationCheck extends IgniteAwareApplication {
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    protected void run(JsonNode jsonNode) throws Exception {
//        log.info("Creating cache...");
//
//        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();
//        cacheCfg.setName(jsonNode.get("cacheName").asText())
//                .setCacheMode(CacheMode.PARTITIONED)
//                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
//
//        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheCfg);
//
//        assert("data".equals(cache.get(1)));
//        assert("2".equals(cache.get("1")));
////        assert(2, cache.get(12));
////        assertEquals(2L, cache.get(13L));
////        assertEquals("Enum_As_Key", cache.get(TestEnum.A));
////        assertEquals(TestEnum.B, cache.get("Enum_As_Value"));
////        assertEquals(TestEnum.C, cache.get(TestEnum.C));
////        assertEquals(new TestSerializable(42), cache.get("Serializable"));
////        assertEquals("Serializable_As_Key", cache.get(new TestSerializable(42)));
////        assertEquals(new TestExternalizable(42), cache.get("Externalizable"));
////        assertEquals("Externalizable_As_Key", cache.get(new TestExternalizable(42)));
////        assertEquals(new TestStringContainerToBePrinted("testStringContainer"), cache.get("testStringContainer"));
////        assertEquals(UUID.fromString("B851B870-3BA7-4E5F-BDB8-458B42300000"),
////                cache.get(UUID.fromString("DA9E0049-468C-4680-BF85-D5379164FDCC")));
//        log.info("Cache created");
//        markSyncExecutionComplete();
//    }
//}
