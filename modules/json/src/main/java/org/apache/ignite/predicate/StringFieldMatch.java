package org.apache.ignite.predicate;

import java.util.Map;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.lang.IgniteBiPredicate;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class StringFieldMatch implements IgniteBiPredicate<Object, Object>{
	private static final long serialVersionUID = 1L;
	
	
	public static enum MatchType {
	    EQUALS, CONTAINS, STARTS
	}
	
	final String field;
	final String matchValue;
	final MatchType matchType;
	final boolean caseSensitive;
	
	public StringFieldMatch(String field, String matchValue,MatchType matchType,boolean caseSensitive) {
		this.field = field;
		this.matchValue = caseSensitive? matchValue: matchValue.toLowerCase();
		this.matchType = matchType;
		this.caseSensitive = caseSensitive;
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
		
		if(targetValue != null) { // all is null
			return filter(matchType, matchValue, caseSensitive ? targetValue.toString(): targetValue.toString().toLowerCase());
		}		
     	return false;						
	}
	
	/**
     * 根据条件进行过滤
     *
     * @param searchText
     * @param searchComment
     * @param matchType
     * @param caseSensitive
     * @return
     */
    public static boolean filter(MatchType matchType, String searchText,String text) {
        if (searchText!=null && !searchText.isEmpty()) {
            if (matchType == MatchType.EQUALS) {
            	if (!text.equals(searchText)) {
                    return false;
                }
            } 
            else if (matchType == MatchType.STARTS) {
            	if (!text.startsWith(searchText)) {
                    return false;
                }
            } 
            else {
            	if (text.indexOf(searchText) < 0) {
                    return false;
                }
            }
        }        
        return true;
    }
}
