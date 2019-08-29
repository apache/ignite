package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.backend.Missing.isNullOrMissing;
import static de.bwaldvogel.mongo.backend.Utils.describeType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntPredicate;

import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.MongoServerError;

interface ExpressionTraits {

    String name();

    default Object requireSingleValue(List<?> list) {
        requireCollectionInSize(list, 1);
        return CollectionUtils.getSingleElement(list);
    }

    default String requireSingleStringValue(List<?> expressionValue) {
        Object value = requireSingleValue(expressionValue);
        if (!(value instanceof String)) {
            throw new MongoServerError(34471, name() + " requires a string argument, found: " + describeType(value));
        }
        return (String) value;
    }

    default Number evaluateNumericValue(List<?> expressionValue, Function<Double, ? extends Number> function) {
        Object value = requireSingleValue(expressionValue);
        if (isNullOrMissing(value)) {
            return null;
        }
        if (!(value instanceof Number)) {
            throw new MongoServerError(28765,
                name() + " only supports numeric types, not " + describeType(value));
        }
        Number number = (Number) value;
        if (Double.isNaN(number.doubleValue())) {
            return number;
        }
        return function.apply(number.doubleValue());
    }

    default int evaluateComparison(List<?> expressionValue) {
        TwoParameters parameters = requireTwoParameters(expressionValue);
        return ValueComparator.ascWithoutListHandling().compare(parameters.getFirst(), parameters.getSecond());
    }

    default boolean evaluateComparison(List<?> expressionValue, IntPredicate comparison) {
        int comparisonResult = evaluateComparison(expressionValue);
        return comparison.test(comparisonResult);
    }

    default <T> T evaluateDateTime(List<?> expressionValue, Function<ZonedDateTime, T> dateFunction, Document document) {
        Object value = requireSingleValue(expressionValue);
        if (isNullOrMissing(value)) {
            return null;
        }

        ZonedDateTime zonedDateTime = getZonedDateTime(value, document);
        return dateFunction.apply(zonedDateTime);
    }

    default <T> T evaluateDate(List<?> expressionValue, Function<LocalDate, T> dateFunction, Document document) {
        return evaluateDateTime(expressionValue, zonedDateTime -> dateFunction.apply(zonedDateTime.toLocalDate()), document);
    }

    default <T> T evaluateTime(List<?> expressionValue, Function<LocalTime, T> timeFunction, Document document) {
        return evaluateDateTime(expressionValue, zonedDateTime -> timeFunction.apply(zonedDateTime.toLocalTime()), document);
    }

    default void requireCollectionInSize(List<?> value, int expectedCollectionSize) {
        if (value.size() != expectedCollectionSize) {
            throw new MongoServerError(16020, "Expression " + name() + " takes exactly " + expectedCollectionSize + " arguments. " + value.size() + " were passed in.");
        }
    }

    default TwoParameters requireTwoParameters(List<?> parameters) {
        requireCollectionInSize(parameters, 2);
        return new TwoParameters(parameters.get(0), parameters.get(1));
    }

    default TwoNumericParameters requireTwoNumericParameters(List<?> value, int errorCode) {
        TwoParameters parameters = requireTwoParameters(value);
        Object one = parameters.getFirst();
        Object other = parameters.getSecond();

        if (parameters.isAnyNull()) {
            return null;
        }

        if (!(one instanceof Number && other instanceof Number)) {
            throw new MongoServerError(errorCode,
                name() + " only supports numeric types, not " + describeType(one) + " and " + describeType(other));
        }

        return new TwoNumericParameters((Number) one, (Number) other);
    }

    default ZonedDateTime getZonedDateTime(Object value, Document document) {
        ZoneId timezone = ZoneId.systemDefault();
        if (value instanceof Document) {
            Document valueAsDocument = (Document) value;
            if (!valueAsDocument.containsKey("date")) {
                throw new MongoServerError(40539, "missing 'date' argument to " + name() + ", provided: " + value);
            }
            value = Expression.evaluate(valueAsDocument.get("date"), document);

            Object timezoneExpression = Expression.evaluate(valueAsDocument.get("timezone"), document);
            if (timezoneExpression != null) {
                timezone = ZoneId.of(timezoneExpression.toString());
            }
        }
        if (!(value instanceof Instant)) {
            throw new MongoServerError(16006, "can't convert from " + describeType(value) + " to Date");
        }

        Instant instant = (Instant) value;
        return ZonedDateTime.ofInstant(instant, timezone);
    }

    default int requireIntegral(Object value, String name) {
        if (!(value instanceof Number)) {
            throw new MongoServerError(40096,
                name() + " requires an integral " + name + ", found a value of type: " + describeType(value) + ", with value: \"" + value + "\"");
        }
        Number number = (Number) value;
        int intValue = number.intValue();
        if (intValue < 0) {
            throw new MongoServerError(40097, name() + " requires a nonnegative " + name + ", found: " + intValue);
        }
        return intValue;
    }

    default <T> Object evaluateIndexOf(List<?> expressionValue,
                                       Function<String, List<T>> toList,
                                       int errorCodeFirstParameterTypeMismatch,
                                       int errorCodeSecondParameterTypeMismatch) {
        if (expressionValue.size() < 2 || expressionValue.size() > 4) {
            throw new MongoServerError(28667,
                "Expression " + name() + " takes at least 2 arguments, and at most 4, but " + expressionValue.size() + " were passed in.");
        }

        Object first = expressionValue.get(0);
        if (isNullOrMissing(first)) {
            return null;
        }
        if (!(first instanceof String)) {
            throw new MongoServerError(errorCodeFirstParameterTypeMismatch,
                name() + " requires a string as the first argument, found: " + describeType(first));
        }
        List<T> elementsToSearchIn = toList.apply((String) first);

        Object searchValue = expressionValue.get(1);
        if (!(searchValue instanceof String)) {
            throw new MongoServerError(errorCodeSecondParameterTypeMismatch,
                name() + " requires a string as the second argument, found: " + describeType(searchValue));
        }
        List<T> search = toList.apply((String) searchValue);

        int start = 0;
        if (expressionValue.size() >= 3) {
            Object startValue = expressionValue.get(2);
            start = requireIntegral(startValue, "starting index");
            start = Math.min(start, elementsToSearchIn.size());
        }

        int end = elementsToSearchIn.size();
        if (expressionValue.size() >= 4) {
            Object endValue = expressionValue.get(3);
            end = requireIntegral(endValue, "ending index");
            end = Math.min(Math.max(start, end), elementsToSearchIn.size());
        }

        elementsToSearchIn = elementsToSearchIn.subList(start, end);
        int index = Collections.indexOfSubList(elementsToSearchIn, search);
        if (index >= 0) {
            return index + start;
        }
        return index;
    }

    default Collection<?> requireArray(int errorCode, Object value) {
        if (!(value instanceof Collection)) {
            throw new MongoServerError(errorCode,
                "The argument to " + name() + " must be an array, but was of type: " + describeType(value));
        }
        return (Collection<?>) value;
    }

    default Document requireDocument(Object expressionValue, int errorCode) {
        if (!(expressionValue instanceof Document)) {
            throw new MongoServerError(errorCode, name() + " only supports an object as its argument");
        }
        return (Document) expressionValue;
    }

    default String evaluateString(List<?> expressionValue, Function<String, String> function) {
        Object value = requireSingleValue(expressionValue);
        value = convertToString(value);
        if (value == null) return null;
        return function.apply((String) value);
    }

    default String convertToString(Object value) {
        if (isNullOrMissing(value)) {
            return null;
        } else if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof ObjectId) {
            return ((ObjectId) value).getHexData();
        }
        throw new MongoServerError(16007, "can't convert from BSON type " + describeType(value) + " to String");
    }

}
