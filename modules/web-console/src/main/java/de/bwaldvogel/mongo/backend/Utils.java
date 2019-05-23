package de.bwaldvogel.mongo.backend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.PathNotViableException;
import de.bwaldvogel.mongo.wire.BsonEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class Utils {

    public static Number addNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double) {
            return Double.valueOf(a.doubleValue() + b.doubleValue());
        } else if (a instanceof Float || b instanceof Float) {
            return Float.valueOf(a.floatValue() + b.floatValue());
        } else if (a instanceof Long || b instanceof Long) {
            return Long.valueOf(a.longValue() + b.longValue());
        } else if (a instanceof Integer || b instanceof Integer) {
            return Integer.valueOf(a.intValue() + b.intValue());
        } else if (a instanceof Short || b instanceof Short) {
            return Short.valueOf((short) (a.shortValue() + b.shortValue()));
        } else {
            throw new UnsupportedOperationException("cannot add " + a + " and " + b);
        }
    }

    public static Number subtractNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double) {
            return Double.valueOf(a.doubleValue() - b.doubleValue());
        } else if (a instanceof Float || b instanceof Float) {
            return Float.valueOf(a.floatValue() - b.floatValue());
        } else if (a instanceof Long || b instanceof Long) {
            return Long.valueOf(a.longValue() - b.longValue());
        } else if (a instanceof Integer || b instanceof Integer) {
            return Integer.valueOf(a.intValue() - b.intValue());
        } else if (a instanceof Short || b instanceof Short) {
            return Short.valueOf((short) (a.shortValue() - b.shortValue()));
        } else {
            throw new UnsupportedOperationException("cannot subtract " + a + " and " + b);
        }
    }

    static Number multiplyNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double) {
            return Double.valueOf(a.doubleValue() * b.doubleValue());
        } else if (a instanceof Float || b instanceof Float) {
            return Float.valueOf(a.floatValue() * b.floatValue());
        } else if (a instanceof Long || b instanceof Long) {
            return Long.valueOf(a.longValue() * b.longValue());
        } else if (a instanceof Integer || b instanceof Integer) {
            return Integer.valueOf(a.intValue() * b.intValue());
        } else if (a instanceof Short || b instanceof Short) {
            return Short.valueOf((short) (a.shortValue() * b.shortValue()));
        } else {
            throw new UnsupportedOperationException("can not multiply " + a + " and " + b);
        }
    }

    public static Object getSubdocumentValue(Document document, String key) {
        if (key.endsWith(".")) {
            throw new MongoServerError(40353, "FieldPath must not end with a '.'.");
        }
        if (key.startsWith(".") || key.contains("..")) {
            throw new MongoServerError(15998, "FieldPath field names may not be empty strings.");
        }
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = key.substring(dotPos + 1);
            Assert.doesNotStartWith(subKey, "$.");
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document) {
                return getSubdocumentValue((Document) subObject, subKey);
            } else {
                return Missing.getInstance();
            }
        } else {
            return Utils.getFieldValueListSafe(document, key);
        }
    }

    public static String getDatabaseNameFromFullName(String fullName) {
        int dotPos = fullName.indexOf('.');
        return fullName.substring(0, dotPos);
    }

    public static String getCollectionNameFromFullName(String fullName) {
        int dotPos = fullName.indexOf('.');
        return fullName.substring(dotPos + 1);
    }

    public static boolean isTrue(Object value) {
        if (Missing.isNullOrMissing(value)) {
            return false;
        }

        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0;
        }

        return true;
    }

    static Object normalizeValue(Object value) {
        if (Missing.isNullOrMissing(value)) {
            return null;
        }
        if (value instanceof Number) {
            double doubleValue = ((Number) value).doubleValue();
            if (doubleValue == -0.0) {
                doubleValue = 0.0;
            }
            return Double.valueOf(doubleValue);
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            Document result = new Document();
            for (Entry<String, Object> entry : map.entrySet()) {
                result.put(entry.getKey(), normalizeValue(entry.getValue()));
            }
            return result;
        } else if (value instanceof Collection<?>) {
            Collection<?> collection = (Collection<?>) value;
            return collection.stream()
                .map(Utils::normalizeValue)
                .collect(Collectors.toList());
        } else {
            return value;
        }
    }

    public static Number normalizeNumber(Number value) {
        if (value == null) {
            return null;
        }

        double doubleValue = value.doubleValue();
        if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
            return Double.valueOf(doubleValue);
        } else if (value.intValue() == doubleValue) {
            return value.intValue();
        } else if (value.longValue() == doubleValue) {
            return value.longValue();
        } else {
            return Double.valueOf(doubleValue);
        }
    }

    static boolean nullAwareEquals(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (Missing.isNullOrMissing(a) && Missing.isNullOrMissing(b)) {
            return true;
        } else if (Missing.isNullOrMissing(a) || Missing.isNullOrMissing(b)) {
            return false;
        } else if (a instanceof byte[] && b instanceof byte[]) {
            byte[] bytesA = (byte[]) a;
            byte[] bytesB = (byte[]) b;
            return Arrays.equals(bytesA, bytesB);
        } else {
            Object normalizedA = normalizeValue(a);
            Object normalizedB = normalizeValue(b);
            return Objects.equals(normalizedA, normalizedB);
        }
    }

    static int calculateSize(Document document) {
        ByteBuf buffer = Unpooled.buffer();
        try {
            new BsonEncoder().encodeDocument(document, buffer);
            return buffer.writerIndex();
        } catch (IOException e) {
            throw new MongoServerException("Failed to calculate document size", e);
        } finally {
            buffer.release();
        }
    }

    static boolean containsQueryExpression(Object value) {
        if (value == null) {
            return false;
        }

        if (!(value instanceof Document)) {
            return false;
        }

        Document doc = (Document) value;
        for (String key : doc.keySet()) {
            if (key.startsWith("$")) {
                return true;
            }
            if (containsQueryExpression(doc.get(key))) {
                return true;
            }
        }
        return false;
    }

    static Object getFieldValueListSafe(Object value, String field) throws IllegalArgumentException {
        if (Missing.isNullOrMissing(value)) {
            return Missing.getInstance();
        }

        if (field.equals("$") || field.contains(".")) {
            throw new IllegalArgumentException("illegal field: " + field);
        }

        if (value instanceof List<?>) {
            if (field.matches("\\d+")) {
                int pos = Integer.parseInt(field);
                List<?> list = (List<?>) value;
                if (pos >= 0 && pos < list.size()) {
                    return list.get(pos);
                } else {
                    return Missing.getInstance();
                }
            } else {
                throw new IllegalArgumentException("illegal field: " + field);
            }
        } else if (value instanceof Document) {
            Document document = (Document) value;
            return document.getOrMissing(field);
        } else {
            return Missing.getInstance();
        }
    }

    static boolean hasSubdocumentValue(Object document, String key) {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = getSubkey(key, dotPos, new AtomicReference<>());
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document || subObject instanceof List<?>) {
                return hasSubdocumentValue(subObject, subKey);
            } else {
                return false;
            }
        } else {
            return Utils.hasFieldValueListSafe(document, key);
        }
    }

    static boolean canFullyTraverseSubkeyForRename(Object document, String key) {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = getSubkey(key, dotPos, new AtomicReference<>());
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document) {
                return canFullyTraverseSubkeyForRename(subObject, subKey);
            } else if (subObject instanceof Missing) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    static String getSubkey(String key, int dotPos, AtomicReference<Integer> matchPos) {
        String subKey = key.substring(dotPos + 1);

        if (key.matches(".*\\$(\\.).+\\$(\\.).*")) {
            throw new BadValueException("Too many positional (i.e. '$') elements found in path '" + key + "'");
        }

        if (subKey.matches("\\$(\\..+)?")) {
            if (matchPos == null || matchPos.get() == null) {
                throw new BadValueException("The positional operator did not find the match needed from the query.");
            }
            Integer pos = matchPos.getAndSet(null);
            return subKey.replaceFirst("\\$", String.valueOf(pos));
        }
        return subKey;
    }

    static boolean hasFieldValueListSafe(Object document, String field) throws IllegalArgumentException {
        if (document == null) {
            return false;
        }

        if (field.equals("$") || field.contains(".")) {
            throw new IllegalArgumentException("illegal field: " + field);
        }

        if (document instanceof List<?>) {
            if (field.matches("\\d+")) {
                int pos = Integer.parseInt(field);
                List<?> list = (List<?>) document;
                return (pos >= 0 && pos < list.size());
            } else {
                return false;
            }
        } else if (document instanceof Document) {
            return ((Document) document).containsKey(field);
        }

        throw new IllegalArgumentException("illegal document: " + document);
    }

    public static void markOkay(Document result) {
        result.put("ok", 1.0);
    }

    private static void setListSafe(Object document, String key, Object obj) {
        if (document instanceof List<?>) {
            int pos = Integer.parseInt(key);
            @SuppressWarnings("unchecked")
            List<Object> list = ((List<Object>) document);
            while (list.size() <= pos) {
                list.add(null);
            }
            list.set(pos, obj);
        } else {
            ((Document) document).put(key, obj);
        }
    }

    private static Object removeListSafe(Object document, String key) {
        if (document instanceof Document) {
            if (((Document) document).containsKey(key)) {
                return ((Document) document).remove(key);
            }
            return Missing.getInstance();
        } else if (document instanceof List<?>) {
            int pos;
            try {
                pos = Integer.parseInt(key);
            } catch (final NumberFormatException e) {
                return Missing.getInstance();
            }

            @SuppressWarnings("unchecked")
            List<Object> list = ((List<Object>) document);
            if (list.size() > pos) {
                return list.set(pos, null);
            } else {
                return null;
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static String join(List<Integer> array, char c) {
        final StringBuilder sb = new StringBuilder();
        for (int value : array) {
            if (sb.length() > 0) {
                sb.append(c);
            }
            sb.append(value);
        }
        return sb.toString();
    }

    static void changeSubdocumentValue(Object document, String key, Object newValue, Integer matchPos) {
        changeSubdocumentValue(document, key, newValue, new AtomicReference<>(matchPos));
    }

    public static void changeSubdocumentValue(Object document, String key, Object newValue) {
        changeSubdocumentValue(document, key, newValue, new AtomicReference<>());
    }

    static void changeSubdocumentValue(Object document, String key, Object newValue, AtomicReference<Integer> matchPos) {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = getSubkey(key, dotPos, matchPos);

            Object subObject = getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document || subObject instanceof List<?>) {
                changeSubdocumentValue(subObject, subKey, newValue, matchPos);
            } else if (!Missing.isNullOrMissing(subObject)) {
                String element = new Document(mainKey, subObject).toString(true);
                throw new PathNotViableException("Cannot create field '" + subKey + "' in element " + element);
            } else {
                Document obj = new Document();
                changeSubdocumentValue(obj, subKey, newValue, matchPos);
                setListSafe(document, mainKey, obj);
            }
        } else {
            setListSafe(document, key, newValue);
        }
    }

    static Object removeSubdocumentValue(Object document, String key, Integer matchPos) {
        return removeSubdocumentValue(document, key, new AtomicReference<>(matchPos));
    }

    public static Object removeSubdocumentValue(Object document, String key) {
        return removeSubdocumentValue(document, key, new AtomicReference<>());
    }

    private static Object removeSubdocumentValue(Object document, String key, AtomicReference<Integer> matchPos) {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = getSubkey(key, dotPos, matchPos);

            Assert.notNullOrEmpty(subKey);

            Object subObject = getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document || subObject instanceof List<?>) {
                return removeSubdocumentValue(subObject, subKey, matchPos);
            } else {
                return Missing.getInstance();
            }
        } else {
            return removeListSafe(document, key);
        }
    }

    public static String describeType(Object value) {
        if (value == null) {
            return "null";
        } else {
            return describeType(value.getClass());
        }
    }

    private static String describeType(Class<?> type) {
        if (Missing.class.isAssignableFrom(type)) {
            return "missing";
        } else if (Document.class.isAssignableFrom(type)) {
            return "object";
        } else if (String.class.isAssignableFrom(type)) {
            return "string";
        } else if (Collection.class.isAssignableFrom(type)) {
            return "array";
        } else if (Integer.class.isAssignableFrom(type)) {
            return "int";
        } else if (Long.class.isAssignableFrom(type)) {
            return "long";
        } else if (Double.class.isAssignableFrom(type)) {
            return "double";
        } else {
            return type.getName();
        }
    }

    static Document cursorResponse(String ns, Document... documents) {
        return cursorResponse(ns, Arrays.asList(documents));
    }

    static Document cursorResponse(String ns, Iterable<Document> documents) {
        List<Document> firstBatch = new ArrayList<>();
        for (Document document : documents) {
            firstBatch.add(document);
        }
        return cursorResponse(ns, firstBatch);
    }

    static Document cursorResponse(String ns, List<Document> firstBatch) {
        Document cursor = new Document();
        cursor.put("id", Long.valueOf(0));
        cursor.put("ns", ns);
        cursor.put("firstBatch", firstBatch);

        Document response = new Document();
        response.put("cursor", cursor);
        markOkay(response);
        return response;
    }
}
