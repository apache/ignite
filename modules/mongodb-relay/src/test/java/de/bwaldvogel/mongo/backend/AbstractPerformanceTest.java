package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.json;

import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.UpdateOptions;

public abstract class AbstractPerformanceTest extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractPerformanceTest.class);

    // https://github.com/bwaldvogel/mongo-java-server/issues/84
    @Test
    public void testComplexUpsert() throws Exception {
        Document incUpdate = new Document();
        Document updateQuery = new Document("$inc", incUpdate);
        incUpdate.put("version", 1);
        for (int hour = 0; hour < 24; hour++) {
            for (int minute = 0; minute < 60; minute++) {
                incUpdate.put("data." + hour + "." + minute + ".requests", 0);
                incUpdate.put("data." + hour + "." + minute + ".responses", 0);
                incUpdate.put("data." + hour + "." + minute + ".duration", 0);
            }
        }

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();
            collection.updateOne(json("_id: 1"), updateQuery, new UpdateOptions().upsert(true));
            long stop = System.currentTimeMillis();
            log.info("Update took {} ms", stop - start);
        }
    }

}
