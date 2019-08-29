package de.bwaldvogel.mongo.bson;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.backend.Missing;

public final class Json {

    private Json() {
    }

    public static String toJsonValue(Object value) {
        return toJsonValue(value, false, "{", "}");
    }

    public static String toJsonValue(Object value, boolean compactKey, String jsonPrefix, String jsonSuffix) {
        if (Missing.isNullOrMissing(value)) {
            return "null";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof String) {
            return "\"" + escapeJson((String) value) + "\"";
        }
        if (value instanceof Document) {
            Document document = (Document) value;
            return document.toString(compactKey, jsonPrefix, jsonSuffix);
        }
        if (value instanceof Instant) {
            Instant instant = (Instant) value;
            return toJsonValue(instant.toString());
        }
        if (value instanceof Collection) {
            Collection<?> collection = (Collection<?>) value;
            if (collection.isEmpty()) {
                return "[]";
            }
            return collection.stream()
                .map(v -> toJsonValue(v, compactKey, "{ ", " }"))
                .collect(Collectors.joining(", ", "[ ", " ]"));
        }
        if (value instanceof ObjectId) {
            ObjectId objectId = (ObjectId) value;
            return objectId.getHexData();
        }
        if (value instanceof UUID) {
            UUID uuid = (UUID) value;
            return "BinData(3, " + toHex(uuid) + ")";
        }
        return toJsonValue(value.toString());
    }

    static String escapeJson(String input) {
        String escaped = input;
        escaped = escaped.replace("\\", "\\\\");
        escaped = escaped.replace("\"", "\\\"");
        escaped = escaped.replace("\b", "\\b");
        escaped = escaped.replace("\f", "\\f");
        escaped = escaped.replace("\n", "\\n");
        escaped = escaped.replace("\r", "\\r");
        escaped = escaped.replace("\t", "\\t");
        return escaped;
    }

    private static StringBuilder toHex(UUID uuid) {
        StringBuilder hex = new StringBuilder();
        byte[] bytes = toBytes(uuid);
        for (int i = bytes.length; i > 0; i--) {
            hex.append(String.format("%02X", bytes[i - 1]));
        }
        return hex;
    }

    private static byte[] toBytes(UUID uuid) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        byteBuffer.putLong(uuid.getMostSignificantBits());
        return byteBuffer.array();
    }

}
