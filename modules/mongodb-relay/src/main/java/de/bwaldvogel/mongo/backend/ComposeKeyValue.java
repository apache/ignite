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

public final class ComposeKeyValue extends KeyValue implements Externalizable {

	private static final long serialVersionUID = 1L;

	private Object[] values;
	private int hashCode = 0;
	
	private ComposeKeyValue() {
		
	}

	ComposeKeyValue(Object... values) {
		super(values);
		this.values = values;
	}


	public int size() {
		return values.length;
	}

	public Object get(int index) {
		return values[index];
	}

	public Object[] iterator() {
		return values;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ComposeKeyValue keyValue = (ComposeKeyValue) o;
		return Arrays.equals(values, keyValue.values);
	}

	@Override
	public int hashCode() {
		if (hashCode == 0) {
			hashCode = Arrays.hashCode(values);
		}
		return hashCode;
	}



	public Stream<Object> stream() {
		return Arrays.stream(values);
	}

	public KeyValue normalized() {
		Object[] normalValue = Arrays.copyOf(values, values.length);
		for (int i = 0; i < normalValue.length; i++) {
			normalValue[i] = Utils.normalizeValue(values[i]);
		}
		return new KeyValue(normalValue);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeObject(this.values);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		this.values = (Object[]) in.readObject();
	}

}