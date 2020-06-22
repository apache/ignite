package de.bwaldvogel.mongo.backend;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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


public class KeyValue implements Externalizable {

    private static final long serialVersionUID = 1L;

    private Object values;   

    protected KeyValue() {
      
    }
    
    public KeyValue(Object value) {
        this.values = value;
    }
    
    public static KeyValue valueOf(Object... value) {
    	if(value.length==1) return new KeyValue(value[0]);
		return new ComposeKeyValue(value);
	}
    
    public int size() {
        return 1;
    }

    public Object get(int index) {
        return values;
    }

   
    public Object[] iterator() {
        return new Object[] { values };
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
        return values.equals(keyValue.values);
    }

    @Override
    public int hashCode() {    
    	return values.hashCode();
    }

    @Override
    public String toString() {
        return stream()
            .map((Object value) -> { return ": " + Json.toJsonValue(value, true, "{ ", " }");})
            .collect(Collectors.joining(", ", "{ ", " }"));
    }

    public Stream<Object> stream() {
        return Stream.of(values);
    }

    public KeyValue normalized() {
    	return new KeyValue(Utils.normalizeValue(values));
    }

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeObject(this.values);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		this.values = in.readObject();
	}
	
}
