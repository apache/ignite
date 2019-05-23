package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;
import static de.bwaldvogel.mongo.backend.Utils.describeType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.ConflictingUpdateOperatorsException;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.ImmutableFieldException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.PathNotViableException;
import de.bwaldvogel.mongo.exception.TypeMismatchException;

class FieldUpdates {

    private final QueryMatcher matcher = new DefaultQueryMatcher();
    private final Map<String, String> renames = new LinkedHashMap<>();
    private final Document document;
    private final String idField;
    private final UpdateOperator updateOperator;
    private final boolean upsert;
    private final Integer matchPos;
    private final ArrayFilters arrayFilters;

    FieldUpdates(Document document, UpdateOperator updateOperator, String idField,
                 boolean upsert, Integer matchPos, ArrayFilters arrayFilters) {
        this.document = document;
        this.idField = idField;
        this.updateOperator = updateOperator;
        this.upsert = upsert;
        this.matchPos = matchPos;
        this.arrayFilters = arrayFilters;
    }

    void apply(Document change, String modifier) {
        for (String key : change.keySet()) {
            Object value = change.get(key);
            if (arrayFilters.isEmpty()) {
                apply(change, modifier, key, value);
            } else {
                List<String> arrayKeys = arrayFilters.calculateKeys(document, key);
                for (String arrayKey : arrayKeys) {
                    apply(change, modifier, arrayKey, value);
                }
            }
        }

        applyRenames();
    }

    private void apply(Document change, String modifier, String key, Object value) {
        switch (updateOperator) {
            case SET_ON_INSERT:
                if (!isUpsert()) {
                    // no upsert → ignore
                    return;
                }
                //$FALL-THROUGH$
            case SET:
                handleSet(key, value);
                break;

            case UNSET:
                handleUnset(key);
                break;

            case PUSH:
            case PUSH_ALL:
            case ADD_TO_SET:
                handlePushAllAddToSet(key, value);
                break;

            case PULL:
            case PULL_ALL:
                handlePull(modifier, key, value);
                break;

            case POP:
                handlePop(modifier, key, value);
                break;

            case INC:
            case MUL:
                handleIncMul(change, key, value);
                break;

            case MIN:
            case MAX:
                handleMinMax(key, value);
                break;

            case CURRENT_DATE:
                handleCurrentDate(key, value);
                break;

            case RENAME:
                handleRename(key, value);
                break;

            default:
                throw new MongoServerError(10147, "Unsupported modifier: " + modifier);
        }
    }

    private void handlePushAllAddToSet(String key, Object changeValue) {
        // http://docs.mongodb.org/manual/reference/operator/push/
        Object value = getSubdocumentValue(document, key);
        final List<Object> list;
        if (Missing.isNullOrMissing(value)) {
            list = new ArrayList<>();
        } else if (value instanceof List<?>) {
            list = asList(value);
        } else {
            if (updateOperator == UpdateOperator.ADD_TO_SET) {
                throw new MongoServerError(10141,
                    "Cannot apply $addToSet to non-array field. Field named '" + key + "' has non-array type " + describeType(value));
            } else if (updateOperator == UpdateOperator.PUSH_ALL || updateOperator == UpdateOperator.PUSH) {
                throw new BadValueException("The field '" + key + "' must be an array but is of type " + describeType(value)
                    + " in document {" + ID_FIELD + ": " + document.get(ID_FIELD) + "}");
            } else {
                throw new IllegalArgumentException("Unsupported operator: " + updateOperator);
            }
        }

        if (updateOperator == UpdateOperator.PUSH_ALL) {
            if (!(changeValue instanceof Collection<?>)) {
                throw new MongoServerError(10153, "Modifier " + updateOperator + " allowed for arrays only");
            }
            @SuppressWarnings("unchecked")
            Collection<Object> valueList = (Collection<Object>) changeValue;
            list.addAll(valueList);
        } else {
            Collection<Object> pushValues = new ArrayList<>();
            if (changeValue instanceof Document
                && ((Document) changeValue).keySet().equals(Collections.singleton("$each"))) {
                @SuppressWarnings("unchecked")
                Collection<Object> values = (Collection<Object>) ((Document) changeValue).get("$each");
                pushValues.addAll(values);
            } else {
                pushValues.add(changeValue);
            }

            for (Object val : pushValues) {
                if (updateOperator == UpdateOperator.PUSH) {
                    list.add(val);
                } else if (updateOperator == UpdateOperator.ADD_TO_SET) {
                    if (!list.contains(val)) {
                        list.add(val);
                    }
                } else {
                    throw new MongoServerException(
                        "internal server error. illegal modifier here: " + updateOperator);
                }
            }
        }
        changeSubdocumentValue(document, key, list);
    }

    private void assertNotKeyField(String key) {
        if (key.equals(idField)) {
            throw new ImmutableFieldException("Performing an update on the path '" + idField + "' would modify the immutable field '" + idField + "'");
        }
    }

    private void handleUnset(String key) {
        assertNotKeyField(key);
        Utils.removeSubdocumentValue(document, key, matchPos);
    }

    private void handleSet(String key, Object newValue) {
        Object oldValue = getSubdocumentValue(document, key);

        if (Utils.nullAwareEquals(newValue, oldValue)) {
            // no change
            return;
        }

        if (!isUpsert()) {
            assertNotKeyField(key);
        }

        changeSubdocumentValue(document, key, newValue);
    }

    private void handlePull(String modifier, String key, Object pullValue) {
        Object value = getSubdocumentValue(document, key);
        final List<Object> list;
        if (Missing.isNullOrMissing(value)) {
            return;
        } else if (value instanceof List<?>) {
            list = asList(value);
        } else {
            if (updateOperator == UpdateOperator.PULL) {
                throw new BadValueException("Cannot apply " + modifier + " to a non-array value");
            } else {
                throw new BadValueException(modifier + " requires an array argument but was given a " + describeType(value));
            }
        }

        if (modifier.equals("$pullAll")) {
            if (!(pullValue instanceof Collection<?>)) {
                throw new BadValueException(modifier + " requires an array argument but was given a " + describeType(pullValue));
            }
            @SuppressWarnings("unchecked")
            Collection<Object> valueList = (Collection<Object>) pullValue;
            list.removeIf(obj -> valueList.stream().anyMatch(v -> matcher.matchesValue(obj, v)));
        } else {
            list.removeIf(obj -> matcher.matchesValue(pullValue, obj));
        }
    }

    private void handlePop(String modifier, String key, Object popValue) {
        Object value = getSubdocumentValue(document, key);
        final List<Object> list;
        if (Missing.isNullOrMissing(value)) {
            return;
        } else if (value instanceof List<?>) {
            list = asList(value);
        } else {
            throw new MongoServerError(10143, modifier + " requires an array argument but was given a " + describeType(value));
        }

        if (popValue == null) {
            throw new FailedToParseException("Expected a number in: " + key + ": null");
        }
        if (!list.isEmpty()) {
            Object normalizedValue = Utils.normalizeValue(popValue);
            Assert.notNull(normalizedValue);
            if (normalizedValue.equals(Double.valueOf(-1.0))) {
                list.remove(0);
            } else {
                list.remove(list.size() - 1);
            }
        }
    }

    private void handleIncMul(Document change, String key, Object changeObject) {
        assertNotKeyField(key);

        Object value = getSubdocumentValue(document, key);
        Number number;
        if (Missing.isNullOrMissing(value)) {
            number = Integer.valueOf(0);
        } else if (value instanceof Number) {
            number = (Number) value;
        } else {
            throw new TypeMismatchException("Cannot apply " + updateOperator.getValue() + " to a value of non-numeric type." +
                " {" + ID_FIELD + ": " + Json.toJsonValue(document.get(ID_FIELD)) + "} has the field '" + key + "'" +
                " of non-numeric type " + describeType(value));
        }

        if (!(changeObject instanceof Number)) {
            String operation = (updateOperator == UpdateOperator.INC) ? "increment" : "multiply";
            throw new TypeMismatchException("Cannot " + operation + " with non-numeric argument: " + change.toString(true));
        }
        Number changeValue = (Number) changeObject;
        final Number newValue;
        if (updateOperator == UpdateOperator.INC) {
            newValue = Utils.addNumbers(number, changeValue);
        } else if (updateOperator == UpdateOperator.MUL) {
            newValue = Utils.multiplyNumbers(number, changeValue);
        } else {
            throw new RuntimeException();
        }

        changeSubdocumentValue(document, key, newValue);
    }

    private void handleMinMax(String key, Object newValue) {
        assertNotKeyField(key);

        Object oldValue = getSubdocumentValue(document, key);

        if (shouldChangeValue(oldValue, newValue)) {
            changeSubdocumentValue(document, key, newValue);
        }
    }

    private void handleCurrentDate(String key, Object typeSpecification) {
        assertNotKeyField(key);

        final boolean useDate;
        if (typeSpecification instanceof Boolean && Utils.isTrue(typeSpecification)) {
            useDate = true;
        } else if (typeSpecification instanceof Document) {
            Object type = ((Document) typeSpecification).get("$type");
            if (type.equals("timestamp")) {
                useDate = false;
            } else if (type.equals("date")) {
                useDate = true;
            } else {
                throw new BadValueException("The '$type' string field is required to be 'date' or 'timestamp': " +
                    "{$currentDate: {field : {$type: 'date'}}}");
            }
        } else {
            throw new BadValueException(describeType(typeSpecification) + " is not valid type for $currentDate." + //
                " Please use a boolean ('true') or a $type expression ({$type: 'timestamp/date'}).");
        }


        final Object newValue;
        if (useDate) {
            newValue = new Date();
        } else {
            newValue = new BsonTimestamp(System.currentTimeMillis());
        }

        changeSubdocumentValue(document, key, newValue);
    }

    private void handleRename(String key, Object toField) {
        assertNotKeyField(key);
        if (!(toField instanceof String)) {
            throw new BadValueException("The 'to' field for $rename must be a string: " + key + ": " + toField);
        }
        String newKey = (String) toField;
        assertNotKeyField(newKey);

        if (renames.containsKey(key) || renames.containsValue(key)) {
            throw new ConflictingUpdateOperatorsException(
                "Updating the path '" + key + "' would create a conflict at '" + key + "'");
        }
        if (renames.containsKey(newKey) || renames.containsValue(newKey)) {
            throw new ConflictingUpdateOperatorsException(
                "Updating the path '" + newKey + "' would create a conflict at '" + newKey + "'");
        }

        renames.put(key, newKey);
    }

    private void applyRenames() {
        for (Entry<String, String> entry : renames.entrySet()) {
            if (!Utils.canFullyTraverseSubkeyForRename(document, entry.getKey())) {
                throw new PathNotViableException("cannot traverse element");
            }

            Object value = Utils.removeSubdocumentValue(document, entry.getKey(), matchPos);

            if (!(value instanceof Missing)) {
                changeSubdocumentValue(document, entry.getValue(), value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object value) {
        return (List<Object>) value;
    }

    private void changeSubdocumentValue(Document document, String key, Object newValue) {
        Utils.changeSubdocumentValue(document, key, newValue, matchPos);
    }

    private Object getSubdocumentValue(Object document, String key) {
        return getSubdocumentValue(document, key, new AtomicReference<>(matchPos));
    }

    private static Object getSubdocumentValue(Object document, String key, AtomicReference<Integer> matchPos) {
        int dotPos = key.indexOf('.');
        if (dotPos > 0) {
            String mainKey = key.substring(0, dotPos);
            String subKey = Utils.getSubkey(key, dotPos, matchPos);
            Object subObject = Utils.getFieldValueListSafe(document, mainKey);
            if (subObject instanceof Document || subObject instanceof List<?>) {
                return getSubdocumentValue(subObject, subKey, matchPos);
            } else {
                return Missing.getInstance();
            }
        } else {
            return Utils.getFieldValueListSafe(document, key);
        }
    }

    private boolean shouldChangeValue(Object oldValue, Object newValue) {
        if (oldValue instanceof Missing) {
            return true;
        }

        int typeComparison = ValueComparator.compareTypes(newValue, oldValue);
        if (typeComparison != 0) {
            if (updateOperator == UpdateOperator.MAX) {
                return typeComparison > 0;
            } else {
                return typeComparison < 0;
            }
        }

        final ValueComparator valueComparator;
        if (updateOperator == UpdateOperator.MIN) {
            valueComparator = ValueComparator.asc();
        } else {
            valueComparator = ValueComparator.desc();
        }

        int valueComparison = valueComparator.compare(newValue, oldValue);
        return valueComparison < 0;
    }

    private boolean isUpsert() {
        return upsert;
    }

}
