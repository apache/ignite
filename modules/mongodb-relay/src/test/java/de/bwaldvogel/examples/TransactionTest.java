package de.bwaldvogel.examples;



import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.ignite.internal.processors.mongo.MongoPluginConfiguration;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.IterableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.session.ServerSession;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.AbstractTest;
import de.bwaldvogel.mongo.backend.TestUtils;
import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


public class TransactionTest {

    private static MongoCollection<Document> collection;
    private static MongoClient client;
    private static MongoServer server;

    @BeforeEach
    public void setUp() {
    	if(server!=null) {
    		return ;
    	}
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
    	collection.drop();
    	//client.getDatabase("testdb").drop();
        //client.close();
        //server.shutdown();
    }

    @Test
    void testSimpleCursor() {
        int expectedCount = 20;
        int batchSize = 10;
        for (int i = 0; i < expectedCount; i++) {
            collection.insertOne(new Document("_id", 100 + i));
        }
        MongoCursor<Document> cursor = collection.find().batchSize(batchSize).cursor();
        List<Document> retrievedDocuments = new ArrayList<>();
        while (cursor.hasNext()) {
            retrievedDocuments.add(cursor.next());
        }

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(cursor::next)
            .withMessage(null);

    }
    
    // see https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testBuildData() throws Exception {        
        
        ClientSession session = client.startSession();        
       
        BsonDocument id = session.getServerSession().getIdentifier();
        
        session.startTransaction();

        collection.insertOne(json("_id: 1, text: 'abc'"));
        collection.insertOne(json("_id: 2, text: 'def'"));
        collection.insertOne(json("_id: 3, title: '标题'"));
        collection.insertOne(json("_id: 4"));
        collection.insertOne(json("_id: 5, text: 'def'"));

        
        collection.deleteOne(json("_id: 5"));

        collection.updateOne(json("_id: 2"), new Document("$set", json("text: null")));
        collection.updateOne(json("_id: 1"), new Document("$set", json("text: 'def'")));
        
        session.notifyMessageSent();
        
        session.abortTransaction();
        
        session.close();

        FindIterable<Document>  ret = collection.find(json("$text: {$search: 'def'}"));
        MongoCursor<Document> it = ret.cursor();
        while(it.hasNext()) {
        	Document doc = it.next();
        	System.out.println(doc);
        }
        
        System.out.println("finish full text");
    }
    

    protected static <T> IterableAssert<T> assertThat(Iterable<T> actual) {
        // improve assertion array by collection entire array
        List<T> values = TestUtils.toArray(actual);
        return Assertions.assertThat((Iterable<? extends T>) values);
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