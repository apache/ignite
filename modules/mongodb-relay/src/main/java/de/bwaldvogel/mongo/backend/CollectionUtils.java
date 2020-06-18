package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class CollectionUtils {

    private CollectionUtils() {
    }

    public static <T> T getSingleElement(Iterable<T> iterable) {
        Iterator<T> iterator = iterable.iterator();
        Assert.isTrue(iterator.hasNext(), () -> "Expected one element but got zero");
        T value = iterator.next();
        Assert.isFalse(iterator.hasNext(), () -> "Expected one element but got at least two");
        return value;
    }

    public static <T> T getLastElement(List<T> list) {
        Assert.notEmpty(list);
        return list.get(list.size() - 1);
    }

    public static <T> T getSingleElement(Iterable<T> iterable, Supplier<? extends RuntimeException> exceptionSupplier) {
        Iterator<T> iterator = iterable.iterator();
        if (!iterator.hasNext()) {
            throw exceptionSupplier.get();
        }
        T value = iterator.next();
        if (iterator.hasNext()) {
            throw exceptionSupplier.get();
        }
        return value;
    }

    static <T> List<List<T>> multiplyWithOtherElements(Collection<?> allValues, Collection<T> collectionValues) {
        Assert.isTrue(allValues.contains(collectionValues), () -> "Expected " + collectionValues + " to be part of " + allValues);
        List<List<T>> result = new ArrayList<>();
        for (T collectionValue : collectionValues) {
            List<T> values = new ArrayList<>();

            for (Object value : allValues) {
                if (value == collectionValues) {
                    values.add(collectionValue);
                } else {
                    values.add((T) value);
                }
            }

            result.add(values);
        }
        return result;
    }

    public static <T extends Map<K, ?>, K> T removeAll(T map, Collection<K> keysToRemove) {
        keysToRemove.forEach(map::remove);
        return map;
    }

    static <T> T getElementAtPosition(Iterable<T> iterable, int pos) {
        if (iterable instanceof List) {
            List<T> list = (List<T>) iterable;
            return list.get(pos);
        } else {
            Iterator<T> iterator = iterable.iterator();
            T element = iterator.next();
            for (int i = 0; i < pos; i++) {
                element = iterator.next();
            }
            return element;
        }
    }

}
