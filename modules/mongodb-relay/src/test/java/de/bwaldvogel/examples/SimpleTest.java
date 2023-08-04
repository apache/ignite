package de.bwaldvogel.examples;



import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;

import org.apache.ignite.internal.processors.mongo.MongoPluginConfiguration;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.AbstractTest;
import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


public class SimpleTest {

    private MongoCollection<Document> collection;
    private MongoClient client;
    private MongoServer server;

    @BeforeEach
    public void setUp() {
        server = new MongoServer(IgniteBackend.inMemory(new MongoPluginConfiguration()));

        // bind on a random local port
        String serverAddress = server.bindAndGetConnectionString();

        client = MongoClients.create(serverAddress);
        
        for(String name:client.getDatabase("testdb").listCollectionNames()) {
        	System.out.println(name);
        }
        collection = client.getDatabase("testdb").getCollection("testcoll");
    }

    @AfterEach
    public void tearDown() {
    	client.getDatabase("testdb").drop();
        client.close();
        server.shutdown();
    }

    //@Test
    public void testSimpleInsertQuery() throws Exception {
        assertEquals(0, collection.countDocuments());

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("key", "value");
        collection.insertOne(obj);

        assertEquals(1, collection.countDocuments());
        assertEquals(obj, collection.find().first());
    }
    
    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testSecondarySparseFullTextIndex() throws Exception {
        collection.createIndex(json("text: 'text'"), new IndexOptions().unique(false).sparse(true));        
        collection.createIndex(json("title: 'text', tag: 'text'"), new IndexOptions().unique(false).sparse(true));

        collection.insertOne(json("_id: 1, text: 'abc'"));
        collection.insertOne(json("_id: 2, text: 'def'"));
        collection.insertOne(json("_id: 3, title: '标题'"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, text: 'def'"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 6, text: null")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, text: 'abc def 标题'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: \"abc\" }");

        
        collection.deleteOne(json("_id: 5"));

        collection.updateOne(json("_id: 2"), new Document("$set", json("text: null")));
        collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'")));
        
        

        FindIterable<Document>  ret = collection.find(json("$text: {$search: 'def'}"));
        MongoCursor<Document> it = ret.cursor();
        while(it.hasNext()) {
        	Document doc = it.next();
        	System.out.println(doc);
        }
        
        collection.updateOne(json("_id: 2"), new Document("$set", json("text: 'def',title:'这个 是 标题'")));
        ret = collection.find(json("title: {$text: '标题'}"));
        it = ret.cursor();
        while(it.hasNext()) {
        	Document doc = it.next();
        	System.out.println(doc);
        }
        System.out.println("finish full text");
    }
    

    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testSecondarySparseVectorIndex() throws Exception {
        collection.createIndex(json("text: 'knnVector'"), new IndexOptions().unique(false).sparse(false));

        collection.insertOne(json("_id: 1, text: '中国人'"));
        collection.insertOne(json("_id: 2, text: 'define rule'"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, text: 'China people'"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 6, text: '大秦帝国'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, text: '日本东京真繁华'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: \"abc\" }");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 2"), new Document("$set", json("text: null"))),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        collection.deleteOne(json("_id: 4"));

        collection.updateOne(json("_id: 3"), new Document("$set", json("text: '瓷器国 China'")));
        //collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'")));
        
        for(int i=0;i<10;i++) {
        	System.in.read();
        }
        
        FindIterable<Document>  ret = collection.find(json("$text: {$search: 'China', $score: 1}"));
        MongoCursor<Document> it = ret.cursor();
        while(it.hasNext()) {
        	Document doc = it.next();
        	System.out.println(doc);
        }

        ret = collection.find(json("text: {$text: 'China', $score: 1}"));
        it = ret.cursor();
        while(it.hasNext()) {
        	Document doc = it.next();
        	System.out.println(doc);
        }
    }
    
    

    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testSecondarySparseUniqueIndex() throws Exception {
        collection.createIndex(json("text: 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 1, text: 'abc'"));
        collection.insertOne(json("_id: 2, text: 'def'"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, text: null"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 6, text: null")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, text: 'abc'")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: \"abc\" }");

        assertMongoWriteException(() -> collection.updateOne(json("_id: 2"), new Document("$set", json("text: null"))),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: text_1 dup key: { text: null }");

        collection.deleteOne(json("_id: 5"));

        collection.updateOne(json("_id: 2"), new Document("$set", json("text: null")));
        collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'")));

        collection.deleteMany(json("text: null"));

        FindIterable<Document>  ret = collection.find();
        MongoCursor<Document> it = ret.cursor();
        while(it.hasNext()) {
        	Document doc = it.next();
        	System.out.println(doc);
        }
    }
    
    
    

    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testCompoundSparseUniqueIndex() throws Exception {
        collection.createIndex(json("a: 1, b: 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 1, a: 10, b: 20"));
        collection.insertOne(json("_id: 2, a: 10"));
        collection.insertOne(json("_id: 3, b: 20"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5"));
        collection.insertOne(json("_id: 6, a: null"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, a: null")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: null, b: null }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 7, b: null")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a_1_b_1 dup key: { a: null, b: null }");

        collection.deleteMany(json("a: null, b: null"));

        FindIterable<Document>  ret = collection.find();
        MongoCursor<Document> it = ret.cursor();
        while(it.hasNext()) {
        	Document doc = it.next();
        	System.out.println(doc);
        }
    }

    @Test
    public void testCompoundSparseUniqueIndexOnEmbeddedDocuments() throws Exception {
        collection.createIndex(json("'x.x': 1, 'z.x': 1"), new IndexOptions().unique(true).sparse(true));

        collection.insertOne(json("_id: 1, a: 10, b: 20"));
        collection.insertOne(json("_id: 2, a: 10"));
        collection.insertOne(json("_id: 3, b: 20"));
        collection.insertOne(json("_id: 4, x: {x: 1}"));
        collection.insertOne(json("_id: 5, x: {x: 2}"));
        collection.insertOne(json("_id: 6, x: {x: 1}, z: {x: 2}"));
        collection.insertOne(json("_id: 7, x: {x: 2}, z: {x: 2}"));
        collection.insertOne(json("_id: 8, x: {x: null}, z: {x: null}"));
        collection.insertOne(json("_id: 9"));

        assertMongoWriteException(() -> collection.insertOne(json("_id: 10, x: {x: 1.0}, z: {x: 2.0}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.x_1_b.x_1 dup key: { a.x: 1.0, b.x: 2.0 }");

        assertMongoWriteException(() -> collection.insertOne(json("_id: 11, x: {x: null}, z: {x: null}")),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index: a.x_1_b.x_1 dup key: { a.x: null, b.x: null }");

        collection.deleteMany(json("x: {x: null}, z: {x: null}"));
        collection.deleteMany(json("a: 10"));
        collection.deleteMany(json("b: 20"));

        FindIterable<Document>  ret = collection.find();
        MongoCursor<Document> it = ret.cursor();
        while(it.hasNext()) {
        	Document doc = it.next();
        	System.out.println(doc);
        }
    }
    

    protected static void assertMongoWriteException(ThrowingCallable callable, int expectedErrorCode, String expectedMessage) {
        assertMongoWriteException(callable, expectedErrorCode, "Location" + expectedErrorCode, expectedMessage);
    }

    protected static void assertMongoWriteException(ThrowingCallable callable, int expectedErrorCode, String expectedCodeName,
                                                    String expectedMessage) {
    	try {
    		callable.call();
    	}
    	catch(MongoWriteException e) {
    		e.printStackTrace();
    	}
    	catch (Throwable e1) {
    		e1.printStackTrace();
			
		}        
    }

}