package de.bwaldvogel.mongo.backend;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.BadValueException;

enum BsonType {

    DOUBLE(1, "double", Double.class),
    STRING(2, "string", String.class),
    OBJECT(3, "object", Document.class),
    ARRAY(4, "array", Collection.class),
    BIN_DATA(5, "binData", byte[].class, UUID.class),
    OBJECT_ID(7, "objectId", ObjectId.class),
    BOOL(8, "bool", Boolean.class),
    DATE(9, "date", Instant.class),
    NULL(10, "null") {
        @Override
        public boolean matches(Object value) {
            return value == null;
        }
    },
    REGEX(11, "regex", Pattern.class),
    INT(16, "int", Integer.class),
    TIMESTAMP(17, "timestamp", BsonTimestamp.class),
    LONG(18, "long", Long.class),
    DECIMAL128(19, "decimal", Decimal128.class),
    MIN_KEY(-1, "minKey", MinKey.class),
    MAX_KEY(127, "maxKey", MaxKey.class),
    ;

    private final int number;
    private final String alias;
    private final Set<Class<?>> classes;

    BsonType(int number, String alias, Class<?>... classes) {
        this.number = number;
        this.alias = alias;
        this.classes = new LinkedHashSet<>(Arrays.asList(classes));
    }

    public static BsonType forString(String value) {
        for (BsonType bsonType : values()) {
            if (bsonType.getAlias().equals(value)) {
                return bsonType;
            }
        }
        throw new BadValueException("Unknown type name alias: " + value);
    }

    public static BsonType forNumber(Number value) {
        int type = value.intValue();

        if ((double) type != value.doubleValue()) {
            throw new BadValueException("Invalid numerical type code: " + value);
        }
        for (BsonType bsonType : values()) {
            if (bsonType.getNumber() == type) {
                return bsonType;
            }
        }
        throw new IllegalArgumentException("Unknown type: " + value);
    }

    public boolean matches(Object value) {
        if (value == null) {
            return false;
        }
        return classes.stream().anyMatch(clazz -> clazz.isAssignableFrom(value.getClass()));
    }

    public int getNumber() {
        return number;
    }

    public String getAlias() {
        return alias;
    }
}
