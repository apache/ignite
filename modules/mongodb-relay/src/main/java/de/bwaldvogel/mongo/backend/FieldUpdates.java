package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;
import static de.bwaldvogel.mongo.backend.Utils.describeType;
import static de.bwaldvogel.mongo.backend.Utils.splitPath;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.ConflictingUpdateOperatorsException;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.ImmutableFieldException;
import de.bwaldvogel.mongo.exception.MongoServerError;
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
            if (arrayFilters.canHandle(key)) {
                List<String> arrayKeys = arrayFilters.calculateKeys(document, key);
                for (String arrayKey : arrayKeys) {
                    apply(change, modifier, arrayKey, value);
                }
            } else {
                apply(change, modifier, key, value);
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
                handlePush(key, value);
                break;

            case ADD_TO_SET:
                handleAddToSet(key, value);
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

    private List<Object> getListOrThrow(String key, Function<Object, MongoServerError> errorSupplier) {
        Object value = getSubdocumentValue(document, key);
        if (Missing.isNullOrMissing(value)) {
            return new ArrayList<>();
        } else if (value instanceof List<?>) {
            return asList(value);
        } else {
            throw errorSupplier.apply(value);
        }
    }

    private void handlePush(String key, Object changeValue) {
        List<Object> existingValue = getListOrThrow(key, value ->
            new BadValueException("The field '" + key + "' must be an array but is of type " + describeType(value)
                + " in document {" + ID_FIELD + ": " + document.get(ID_FIELD) + "}"));

        List<Object> newValues = new ArrayList<>();

        Integer slice = null;
        Comparator<Object> comparator = null;
        int position = existingValue.size();
        if (changeValue instanceof Document && ((Document) changeValue).containsKey("$each")) {
            Document pushDocument = (Document) changeValue;
            for (Entry<String, Object> entry : pushDocument.entrySet()) {
                String modifier = entry.getKey();
                switch (modifier) {
                    case "$each":
                        @SuppressWarnings("unchecked")
                        Collection<Object> values = (Collection<Object>) entry.getValue();
                        newValues.addAll(values);
                        break;
                    case "$slice":
                        Object sliceValue = entry.getValue();
                        if (!(sliceValue instanceof Number)) {
                            throw new BadValueException("The value for $slice must be an integer value but was given type: " + describeType(sliceValue));
                        }
                        slice = ((Number) sliceValue).intValue();
                        break;
                    case "$sort":
                        Object sortValue = Utils.normalizeValue(entry.getValue());
                        if (sortValue instanceof Number) {
                            Number sortOrder = Utils.normalizeNumber((Number) sortValue);
                            if (sortOrder.equals(1)) {
                                comparator = ValueComparator.asc();
                            } else if (sortOrder.equals(-1)) {
                                comparator = ValueComparator.desc();
                            }
                        } else if (sortValue instanceof Document) {
                            ValueComparator valueComparator = ValueComparator.asc();
                            DocumentComparator documentComparator = new DocumentComparator((Document) sortValue);
                            comparator = (o1, o2) -> {
                                if (o1 instanceof Document && o2 instanceof Document) {
                                    return documentComparator.compare((Document) o1, (Document) o2);
                                } else if (o1 instanceof Document || o2 instanceof Document) {
                                    return valueComparator.compare(o1, o2);
                                } else {
                                    return 0;
                                }
                            };
                        }
                        if (comparator == null) {
                            throw new BadValueException("The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields");
                        }
                        break;
                    case "$position":
                        if (!(entry.getValue() instanceof Number)) {
                            throw new BadValueException("The value for $position must be an integer value, not of type: " + describeType(entry.getValue()));
                        }
                        position = Utils.normalizeNumber((Number) entry.getValue()).intValue();
                        break;
                    default:
                        throw new BadValueException("Unrecognized clause in $push: " + modifier);
                }
            }
        } else {
            newValues.add(changeValue);
        }

        List<Object> newValue = new ArrayList<>(existingValue);
        if (position < 0) {
            position = newValue.size() + position;
        } else if (position >= newValue.size()) {
            position = newValue.size();
        }
        newValue.addAll(position, newValues);

        if (comparator != null) {
            newValue.sort(comparator);
        }
        if (slice != null) {
            newValue = newValue.subList(0, Math.min(slice.intValue(), newValue.size()));
        }
        changeSubdocumentValue(document, key, newValue);
    }

    private void handleAddToSet(String key, Object changeValue) {
        List<Object> newValue = getListOrThrow(key, value ->
            new MongoServerError(10141,
                "Cannot apply $addToSet to non-array field. Field named '" + key + "' has non-array type " + describeType(value)));

        Collection<Object> pushValues = new ArrayList<>();
        if (changeValue instanceof Document && ((Document) changeValue).keySet().iterator().next().equals("$each")) {
            Document addToSetDocument = (Document) changeValue;
            for (String modifier : addToSetDocument.keySet()) {
                if (!modifier.equals("$each")) {
                    throw new BadValueException("Found unexpected fields after $each in $addToSet: " + addToSetDocument.toString(true, "{ ", " }"));
                }
            }
            @SuppressWarnings("unchecked")
            Collection<Object> values = (Collection<Object>) addToSetDocument.get("$each");
            pushValues.addAll(values);
        } else {
            pushValues.add(changeValue);
        }

        for (Object val : pushValues) {
            if (!newValue.contains(val)) {
                newValue.add(val);
            }
        }
        changeSubdocumentValue(document, key, newValue);
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

        if (!(popValue instanceof Number)) {
            throw new FailedToParseException("Expected a number in: " + key + ": " + Json.toJsonValue(popValue));
        }
        Object normalizedValue = Utils.normalizeValue(popValue);
        Assert.notNull(normalizedValue);
        if (!Utils.nullAwareEquals(normalizedValue, 1) && !Utils.nullAwareEquals(normalizedValue, -1)) {
            throw new FailedToParseException("$pop expects 1 or -1, found: " + Json.toJsonValue(popValue));
        }

        if (!list.isEmpty()) {
            if (Utils.nullAwareEquals(normalizedValue, -1)) {
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
            String lastKey = Utils.getLastFragment(key);
            throw new TypeMismatchException("Cannot apply " + updateOperator.getValue() + " to a value of non-numeric type." +
                " {" + ID_FIELD + ": " + Json.toJsonValue(document.get(ID_FIELD)) + "} has the field '" + lastKey + "'" +
                " of non-numeric type " + describeType(value));
        }

        if (!(changeObject instanceof Number)) {
            String operation = (updateOperator == UpdateOperator.INC) ? "increment" : "multiply";
            throw new TypeMismatchException("Cannot " + operation + " with non-numeric argument: " + change.toString(true));
        }
        Number changeValue = (Number) changeObject;
        final Number newValue;
        if (updateOperator == UpdateOperator.INC) {
            newValue = NumericUtils.addNumbers(number, changeValue);
        } else if (updateOperator == UpdateOperator.MUL) {
            newValue = NumericUtils.multiplyNumbers(number, changeValue);
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

        Instant now = Instant.now();
        final Object newValue;
        if (useDate) {
            newValue = now;
        } else {
            newValue = new Timestamp(now.getEpochSecond());
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
            throw new ConflictingUpdateOperatorsException(key, key);
        }
        if (renames.containsKey(newKey) || renames.containsValue(newKey)) {
            throw new ConflictingUpdateOperatorsException(newKey, newKey);
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
        List<String> pathFragments = splitPath(key);
        String mainKey = pathFragments.get(0);
        if (pathFragments.size() == 1) {
            return Utils.getFieldValueListSafe(document, mainKey);
        }
        String subKey = Utils.getSubkey(pathFragments, matchPos);
        Object subObject = Utils.getFieldValueListSafe(document, mainKey);
        if (subObject instanceof Document || subObject instanceof List<?>) {
            return getSubdocumentValue(subObject, subKey, matchPos);
        } else {
            return Missing.getInstance();
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
