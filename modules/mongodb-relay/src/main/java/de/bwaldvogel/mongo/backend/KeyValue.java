package de.bwaldvogel.mongo.backend;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.bson.Json;

public final class KeyValue implements Serializable, Iterable<Object> {

    private static final long serialVersionUID = 1L;

    private final List<Object> values;

    public KeyValue(Object... values) {
        this(Arrays.asList(values));
    }

    public KeyValue(Object values) {
        this(Collections.singletonList(values));
    }
    
    public KeyValue(Collection<?> values) {
        Assert.notEmpty(values);
        this.values = new ArrayList<>(values);
    }

    public int size() {
        return values.size();
    }

    public Object get(int index) {
        return values.get(index);
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyValue keyValue = (KeyValue) o;
        return Objects.equals(values, keyValue.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return values.stream()
            .map(value -> ": " + Json.toJsonValue(value, true, "{ ", " }"))
            .collect(Collectors.joining(", ", "{ ", " }"));
    }

    public Stream<Object> stream() {
        return values.stream();
    }

    public KeyValue normalized() {
    	if(values.size()==1) {
    		return new KeyValue(Utils.normalizeValue(values.get(0)));
    	}
        return new KeyValue(values.stream()
            .map(Utils::normalizeValue)
            .collect(Collectors.toList()));
    }

}
