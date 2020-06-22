package de.bwaldvogel.examples;


import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

public class SimpleTest {

    private MongoCollection<Document> collection;
    private MongoClient client;
    private MongoServer server;

    @Before
    public void setUp() {
        //server = new MongoServer(IgniteBackend.inMemory());

        // bind on a random local port
        //InetSocketAddress serverAddress = server.bind();

        client = new MongoClient("127.0.0.1:27018");
        
        for(String name:client.getDatabase("default").listCollectionNames()) {
        	System.out.println(name);
        }
        collection = client.getDatabase("default").getCollection("testcollection");
    }

    @After
    public void tearDown() {
        client.close();
        server.shutdown();
    }

    @Test
    public void testSimpleInsertQuery() throws Exception {
        assertEquals(0, collection.count());

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("key", "value");
        collection.insertOne(obj);

        assertEquals(1, collection.count());
        assertEquals(obj, collection.find().first());
    }

}