package de.bwaldvogel.mongo.backend.ignite.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;

import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.QueryFilter;
import de.bwaldvogel.mongo.bson.Document;

public class BinaryObjectMatch extends DefaultQueryMatcher implements IgniteBiPredicate<Object, Object>{
	private static final long serialVersionUID = 1L;	
	
	private final Document query;
	private final String idField;
	
	public BinaryObjectMatch(Document query, String idField) {		
		this.query = query;
		this.idField = idField;
	}

	@Override 
	public boolean apply(Object key, Object other) {		
     	// Document document = DocumentUtil.binaryObjectToDocument(key,other,idField,query.keySet());
		if(other instanceof BinaryObject) {
			if (this.matches((BinaryObject)other, query)) {
	            return true;
	        }
			return false;
		}
     	return true;
     }
	
	/**
	 * 预过滤，不确定的情况下返回true
	 * @param document
	 * @param query
	 * @return
	 */
    public boolean matches(BinaryObject document, Document query) {
        for (String key : query.keySet()) {
            Object queryValue = query.get(key);
            //-validateQueryValue(queryValue, key);
            List<String> keys = splitKey(key);
            if(keys.size()!=1) {
            	continue;
            }
            if (QueryFilter.isQueryFilter(key)) {
            	continue;
            }
            if (key.startsWith("$")) {
            	continue;
            }
            if(!document.hasField(key)) {
            	continue;
            }
            Object documentValue = document.field(key);
            if (documentValue instanceof Collection<?>) {
                Collection<?> documentValues = (Collection<?>) documentValue;
                if (queryValue instanceof Document) {
                    Document queryDocument = (Document) queryValue;
                    boolean matches = checkMatchesAnyValue(queryDocument, keys, null, documentValues);
                    if (matches) {
                    	continue;
                    }                    
                    if (isInQuery(queryDocument)) {
                    	matches = checkMatchesValue(queryDocument, documentValue);
                    	if (matches) {
                        	continue;
                        }
                    	return false;
                    } else {
                        return false;
                    }
                } else if (queryValue instanceof Collection<?>) {
                	boolean matches = checkMatchesValue(queryValue, documentValues);
                	if (matches) {
                    	continue;
                    }
                	return false;
                } else if (!checkMatchesAnyValue(queryValue, documentValues)) {
                    return false;
                }
            }
            else if (documentValue instanceof Map && !(documentValue instanceof Document)) {
            	documentValue = new Document((Map)documentValue);
            }

            return checkMatchesValue(queryValue, documentValue);
        }

        return true;
    }    

}
