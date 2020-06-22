package de.bwaldvogel.mongo;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.bson.Document;

public class TestUtils {

    public static Document json(String string) {
        string = string.trim();
        if (!string.startsWith("{")) {
            string = "{" + string + "}";
        }
        return toDocument(org.bson.Document.parse(string));
    }

    private static Document toDocument(Map<String, Object> map) {
        Document document = new Document();
        for (Entry<String, Object> entry : map.entrySet()) {
            document.put(entry.getKey(), convertValue(entry.getValue()));
        }
        return document;
    }

    private static Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mapValue = (Map<String, Object>) value;
            return toDocument(mapValue);
        }
        if (value instanceof Collection<?>) {
            Collection<?> collection = (Collection<?>) value;
            return collection.stream()
                .map(TestUtils::convertValue)
                .collect(Collectors.toList());
        }
        return value;
    }

}
