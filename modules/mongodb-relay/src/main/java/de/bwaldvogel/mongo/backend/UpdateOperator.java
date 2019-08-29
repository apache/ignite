package de.bwaldvogel.mongo.backend;

import java.util.HashMap;
import java.util.Map;

// see http://docs.mongodb.org/manual/reference/operator/update/
enum UpdateOperator {

    // http://docs.mongodb.org/manual/reference/operator/update-field/
    INC("$inc"), // Increments the value of the field by the specified amount.
    MUL("$mul"), // Multiplies the value of the field by the specified amount.
    RENAME("$rename"), // Renames a field.
    SET_ON_INSERT("$setOnInsert"), // Sets the value of a field if an update results in an insert of a document. Has no effect on update operations that modify existing documents.
    SET("$set"), // Sets the value of a field in a document.
    UNSET("$unset"), // Removes the specified field from a document.
    MIN("$min"), // Only updates the field if the specified value is less than the existing field value.
    MAX("$max"), // Only updates the field if the specified value is greater than the existing field value.
    CURRENT_DATE("$currentDate"), // Sets the value of a field to current date, either as a Date or a Timestamp.

    // http://docs.mongodb.org/manual/reference/operator/update-array/

    ADD_TO_SET("$addToSet"), // Adds elements to an array only if they do not already exist in the set.
    POP("$pop"), // Removes the first or last item of an array.
    PULL_ALL("$pullAll"), // Removes all matching values from an array.
    PULL("$pull"), // Removes all array elements that match a specified query.
    PUSH_ALL("$pushAll"), // Deprecated. Adds several items to an array.
    PUSH("$push"), // Adds an item to an array.

    // http://docs.mongodb.org/manual/reference/operator/update-bitwise/

    BIT("$bit") // Performs bitwise AND, OR, and XOR updates of integer values.

    ;

    private String value;

    UpdateOperator(String value) {
        this.value = value;
    }

    String getValue() {
        return value;
    }

    private static final Map<String, UpdateOperator> MAP = new HashMap<>();

    static {
        for (UpdateOperator operator : UpdateOperator.values()) {
            UpdateOperator old = MAP.put(operator.getValue(), operator);
            Assert.isNull(old, () -> "Duplicate operator value: " + operator.getValue());
        }
    }

    static UpdateOperator fromValue(String value) throws IllegalArgumentException {
        UpdateOperator op = MAP.get(value);
        Assert.notNull(op, () -> "Illegal operator: " + value);
        return op;
    }

    @Override
    public String toString() {
        return getValue();
    }

}
