package de.bwaldvogel.examples;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;



class SimpleJUnit5WithModernClientTest {

    private MongoCollection<Document> collection;
    private MongoClient client;
    

    @BeforeEach
    void setUp() {
       

        // bind on a random local port
        String connectionString = "mongodb://127.0.0.1:27018/graph?ssl=false";

        client = MongoClients.create(connectionString);
        collection = client.getDatabase("graph").getCollection("testcollection");
    }

    @AfterEach
    void tearDown() {
        client.close();        
    }

    //@Test
    void testSimpleInsertQuery() throws Exception {

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 3).append("_key", "value");
        collection.insertOne(obj);
        
     // creates the database and collection in memory and insert the object
        Document obj2 = new Document("_id", 2).append("key", "value2");
        collection.insertOne(obj2);

        assertThat(collection.countDocuments()).isEqualTo(2L);
        assertThat(collection.find(obj).first()).isEqualTo(obj);
    }
    
    @Test
    void testCreateViewQuery() throws Exception {
    	
    	Document filter = new Document().append("$match", new Document().append("key",new Document("$exists",1)));
    	
    	List<Document> pipeline = List.of(filter);
        
    	client.getDatabase("graph").createView("test_view2", "testcollection", pipeline);
    	
    	collection = client.getDatabase("graph").getCollection("test_view2");
    	
    	for(Document doc: collection.find()) {
    		System.out.println(doc);
    	}

        System.out.println();
    }

}
