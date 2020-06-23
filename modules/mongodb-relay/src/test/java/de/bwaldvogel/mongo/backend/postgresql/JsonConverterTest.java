package de.bwaldvogel.mongo.backend.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;

import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;

public class JsonConverterTest {

    @Test
    public void testSerializeAndDeserialize_MinKey() throws Exception {
        String json = JsonConverter.toJson(new Document("_id", MinKey.getInstance()));
        Document document = JsonConverter.fromJson(json);
        assertThat(document.get("_id")).isInstanceOf(MinKey.class);
    }

    @Test
    public void testSerializeAndDeserialize_MaxKey() throws Exception {
        String json = JsonConverter.toJson(new Document("_id", MaxKey.getInstance()));
        Document document = JsonConverter.fromJson(json);
        assertThat(document.get("_id")).isInstanceOf(MaxKey.class);
    }

    
    public static void main(String[] args) throws Exception {
    	JsonConverterTest test = new JsonConverterTest();
    	test.testSerializeAndDeserialize_MinKey();
    	test.testSerializeAndDeserialize_MaxKey();
    	
 
    }

}
