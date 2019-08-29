package de.bwaldvogel.mongo.backend;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;

public class ValueComparator implements Comparator<Object> {

    private static final List<Class<?>> SORT_PRIORITY = new ArrayList<>();

    private static final ValueComparator ASCENDING = new ValueComparator(true);
    private static final ValueComparator DESCENDING = new ValueComparator(false);
    private static final ValueComparator ASCENDING_NO_LIST_HANDLING = new ValueComparator(true, false);

    private final boolean ascending;
    private final boolean handleLists;

    public static ValueComparator asc() {
        return ASCENDING;
    }

    public static ValueComparator ascWithoutListHandling() {
        return ASCENDING_NO_LIST_HANDLING;
    }

    public static ValueComparator desc() {
        return DESCENDING;
    }

    static {
        /*
         * https://docs.mongodb.com/manual/reference/bson-type-comparison-order/
         */
        SORT_PRIORITY.add(Number.class);
        SORT_PRIORITY.add(String.class);
        SORT_PRIORITY.add(Document.class);
        SORT_PRIORITY.add(List.class);
        SORT_PRIORITY.add(byte[].class);
        SORT_PRIORITY.add(UUID.class);
        SORT_PRIORITY.add(ObjectId.class);
        SORT_PRIORITY.add(Boolean.class);
        SORT_PRIORITY.add(Instant.class);
        SORT_PRIORITY.add(BsonRegularExpression.class);
    }

    private ValueComparator(boolean ascending) {
        this(ascending, true);
    }

    private ValueComparator(boolean ascending, boolean handleLists) {
        this.ascending = ascending;
        this.handleLists = handleLists;
    }

    @Override
    public ValueComparator reversed() {
        return ascending ? desc() : asc();
    }

    static int compareTypes(Object value1, Object value2) {
        if (Missing.isNullOrMissing(value1) && Missing.isNullOrMissing(value2)) {
            return 0;
        } else if (Missing.isNullOrMissing(value1)) {
            return -1;
        } else if (Missing.isNullOrMissing(value2)) {
            return 1;
        }

        int t1 = getTypeOrder(value1);
        int t2 = getTypeOrder(value2);
        return Integer.compare(t1, t2);
    }

    @Override
    public int compare(Object o1, Object o2) {
        int cmp = doCompare(o1, o2);
        return ascending ? cmp : -cmp;
    }

    private static int compareAsc(Object value1, Object value2) {
        return asc().compare(value1, value2);
    }

    private int doCompare(Object value1, Object value2) {
        if (value1 == value2) {
            return 0;
        }

        if (handleLists && (value1 instanceof Collection || value2 instanceof Collection)) {
            return compareLists(value1, value2);
        }

        if (Missing.isNullOrMissing(value1) && Missing.isNullOrMissing(value2)) {
            return 0;
        }

        int typeComparision = compareTypes(value1, value2);
        if (typeComparision != 0) {
            return typeComparision;
        }

        Class<?> clazz = value1.getClass();

        if (ObjectId.class.isAssignableFrom(clazz)) {
            return ((ObjectId) value1).compareTo((ObjectId) value2);
        }

        if (value1 instanceof Decimal128 && value2 instanceof Decimal128) {
            Decimal128 decimal1 = (Decimal128) value1;
            Decimal128 decimal2 = (Decimal128) value2;
            return decimal1.compareTo(decimal2);
        }

        if (Number.class.isAssignableFrom(clazz)) {
            Number number1 = Utils.normalizeNumber((Number) value1);
            Number number2 = Utils.normalizeNumber((Number) value2);
            return Double.compare(number1.doubleValue(), number2.doubleValue());
        }

        if (String.class.isAssignableFrom(clazz)) {
            return value1.toString().compareTo(value2.toString());
        }

        if (Instant.class.isAssignableFrom(clazz)) {
            Instant date1 = (Instant) value1;
            Instant date2 = (Instant) value2;
            return date1.compareTo(date2);
        }

        if (Boolean.class.isAssignableFrom(clazz)) {
            boolean b1 = ((Boolean) value1).booleanValue();
            boolean b2 = ((Boolean) value2).booleanValue();
            return (!b1 && b2) ? -1 : (b1 && !b2) ? +1 : 0;
        }

        if (List.class.isAssignableFrom(clazz)) {
            return compareListsForEquality((Collection<?>) value1, (Collection<?>) value2);
        }

        // lexicographic byte comparison 0x00 < 0xFF
        if (clazz.isArray()) {
            Class<?> componentType = clazz.getComponentType();
            if (byte.class.isAssignableFrom(componentType)) {
                byte[] bytes1 = (byte[]) value1;
                byte[] bytes2 = (byte[]) value2;
                if (bytes1.length != bytes2.length) {
                    return Integer.compare(bytes1.length, bytes2.length);
                } else {
                    for (int i = 0; i < bytes1.length; i++) {
                        int compare = compareUnsigned(bytes1[i], bytes2[i]);
                        if (compare != 0) return compare;
                    }
                    return 0;
                }
            }
        }

        if (Document.class.isAssignableFrom(clazz)) {
            return compareDocuments((Document) value1, (Document) value2);
        }

        if (UUID.class.isAssignableFrom(clazz)) {
            UUID uuid1 = (UUID) value1;
            UUID uuid2 = (UUID) value2;
            return uuid1.compareTo(uuid2);
        }

        throw new UnsupportedOperationException("can't compare " + clazz);
    }

    private int compareListsForEquality(Collection<?> value1, Collection<?> value2) {
        Assert.isFalse(handleLists, () -> "Unexpected state");

        List<?> collection1 = new ArrayList<>(value1);
        List<?> collection2 = new ArrayList<>(value2);

        for (int i = 0; i < Math.max(collection1.size(), collection2.size()); i++) {
            Object v1 = i >= collection1.size() ? Missing.getInstance() : collection1.get(i);
            Object v2 = i >= collection2.size() ? Missing.getInstance() : collection2.get(i);
            int cmp = compare(v1, v2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static boolean isEmptyList(Object value1) {
        return value1 instanceof Collection && ((Collection<?>) value1).isEmpty();
    }

    private int compareLists(Object value1, Object value2) {
        Object valueForComparison1 = getListValueForComparison(value1);
        Object valueForComparison2 = getListValueForComparison(value2);

        if (isEmptyList(value1) && Missing.isNullOrMissing(valueForComparison2)) {
            return -1;
        }
        if (isEmptyList(value2) && Missing.isNullOrMissing(valueForComparison1)) {
            return 1;
        }

        return compareAsc(valueForComparison1, valueForComparison2);
    }

    private Object getListValueForComparison(Object value) {
        if (value instanceof Collection) {
            List<Object> values = new ArrayList<> ((Collection<Object>) value);
            if (values.isEmpty()) {
                return Missing.getInstance();
            }
            values.sort(this);
            return values.get(0);
        } else {
            return value;
        }
    }

    private int compareDocuments(Document document1, Document document2) {
        List<String> keys1 = new ArrayList<>(document1.keySet());
        List<String> keys2 = new ArrayList<>(document2.keySet());
        for (int i = 0; i < Math.max(keys1.size(), keys2.size()); i++) {
            String key1 = i >= keys1.size() ? null : keys1.get(i);
            String key2 = i >= keys2.size() ? null : keys2.get(i);

            Object value1 = document1.getOrMissing(key1);
            Object value2 = document2.getOrMissing(key2);

            int typeComparison = compareTypes(value1, value2);
            if (typeComparison != 0) {
                return typeComparison;
            }

            int keyComparison = compareAsc(key1, key2);
            if (keyComparison != 0) {
                return keyComparison;
            }

            int valueComparison = compareAsc(value1, value2);
            if (valueComparison != 0) {
                return valueComparison;
            }
        }

        return 0;
    }

    private static int compareUnsigned(byte b1, byte b2) {
        int v1 = (int) b1 & 0xFF;
        int v2 = (int) b2 & 0xFF;
        return Integer.compare(v1, v2);
    }

    private static int getTypeOrder(Object obj) {
        for (int idx = 0; idx < SORT_PRIORITY.size(); idx++) {
            if (SORT_PRIORITY.get(idx).isAssignableFrom(obj.getClass())) {
                return idx;
            }
        }
        throw new UnsupportedOperationException("can't sort " + obj.getClass());
    }
}
