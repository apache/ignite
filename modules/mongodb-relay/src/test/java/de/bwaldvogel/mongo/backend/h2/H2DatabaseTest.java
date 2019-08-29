package de.bwaldvogel.mongo.backend.h2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import de.bwaldvogel.mongo.backend.IndexKey;

public class H2DatabaseTest {

    @Test
    public void testIndexName() throws Exception {
        try {
            H2Database.indexName(Collections.emptyList());
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertEquals("No keys", e.getMessage());
        }

        assertEquals("key1.ASC", H2Database.indexName(Collections.singletonList(
            new IndexKey("key1", true)
        )));

        assertEquals("key1.ASC_key2.DESC", H2Database.indexName(Arrays.asList(
            new IndexKey("key1", true),
            new IndexKey("key2", false)
        )));

        assertEquals("key1.ASC_key2.DESC_key3.DESC", H2Database.indexName(Arrays.asList(
            new IndexKey("key1", true),
            new IndexKey("key2", false),
            new IndexKey("key3", false)
        )));
    }

}