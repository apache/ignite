package de.bwaldvogel.mongo.backend;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;

import com.mongodb.client.MongoDatabase;

public class TestUtils {

    private TestUtils() {
    }

    public static <T> List<T> toArray(Iterable<T> iterable) {
        List<T> array = new ArrayList<>();
        for (T obj : iterable) {
            array.add(obj);
        }
        return array;
    }

    public static Document json(String string) {
        string = string.trim();
        if (!string.startsWith("{")) {
            string = "{" + string + "}";
        }
        return Document.parse(string);
    }

    static List<Document> jsonList(String... json) {
        return Stream.of(json)
            .map(TestUtils::json)
            .collect(Collectors.toList());
    }

    public static Document getCollectionStatistics(MongoDatabase database, String collectionName) {
        Document collStats = new Document("collStats", collectionName);
        return database.runCommand(collStats);
    }

    static Instant instant(String value) {
        return Instant.parse(value);
    }

    static Date date(String value) {
        return Date.from(instant(value));
    }
}
