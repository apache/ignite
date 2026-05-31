package org.apache.ignite.predicate;

import java.util.Map;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.lang.IgniteBiPredicate;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class FieldEqualsMatch implements IgniteBiPredicate<Object, Object>{
	private static final long serialVersionUID = 1L;
	
	final String field;
	final Object matchValue;
	
	public FieldEqualsMatch(String field, Object matchValue) {
		this.field = field;
		this.matchValue = matchValue;
		
	}
	
	@Override
	public boolean apply(Object k, Object v) {
		Object targetValue = null;
		if(v instanceof BinaryObject) {
			BinaryObject bo = (BinaryObject)v;
			try {
				targetValue = bo.field(field);				
			}
			catch(BinaryObjectException e) {
				// ignore
			}			
		}
		else if(v instanceof Map) {
			Map dict = (Map)v;
			targetValue = dict.get(field);			
		}
		else if(v instanceof ObjectNode) {
			ObjectNode json = (ObjectNode)v;
			targetValue = json.get(field);
		}
		
		if(matchValue == targetValue) { // all is null
			return true;
		}
		if (matchValue!=null && matchValue.equals(targetValue)) {
            return true;
        }
     	return false;						
	}
}
