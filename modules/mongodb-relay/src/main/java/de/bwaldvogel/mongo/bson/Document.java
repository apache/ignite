package de.bwaldvogel.mongo.bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.backend.Missing;

public final class Document implements Map<String, Object>, Bson {

    private static final long serialVersionUID = 1L;

    private final LinkedHashMap<String, Object> documentAsMap;

    public Document() {
    	documentAsMap = new LinkedHashMap<>();
    }

    public Document(String key, Object value) {
    	documentAsMap = new LinkedHashMap<>();
        append(key, value);
    }

    public Document(final Map<String, Object> map) {
    	if(map instanceof LinkedHashMap) {
    		this.documentAsMap = (LinkedHashMap)map;
    	}
    	else {
    		documentAsMap = new LinkedHashMap<>(map.size());
    		putAll(map);
    	}       
    }
   

    public Document cloneDeeply() {
        return cloneDeeply(this);
    }

    @SuppressWarnings("unchecked")
    private static <T> T cloneDeeply(T object) {
        if (object == null) {
            return null;
        } else if (object instanceof Document) {
            Document document = (Document) object;
            Document clone = document.clone();
            for (String key : document.keySet()) {
                clone.put(key, cloneDeeply(clone.get(key)));
            }
            return (T) clone;
        } else if (object instanceof List) {
            List<?> list = (List<?>) object;
            List<?> result = list.stream()
                .map(Document::cloneDeeply)
                .collect(Collectors.toList());
            return (T) result;
        } else if (object instanceof Set) {
            Set<?> set = (Set<?>) object;
            Set<?> result = set.stream()
                .map(Document::cloneDeeply)
                .collect(Collectors.toCollection(LinkedHashSet::new));
            return (T) result;
        } else {
            return object;
        }
    }

    public Document append(String key, Object value) {
        put(key, value);
        return this;
    }

    public Document appendAll(Map<String, Object> map) {
        putAll(map);
        return this;
    }

    @Override
    public boolean containsValue(Object value) {
        return documentAsMap.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return documentAsMap.get(key);
    }

    public Object getOrMissing(Object key) {
        return getOrDefault(key, Missing.getInstance());
    }

    @Override
    public void clear() {
        documentAsMap.clear();
    }

    @Override
    public int size() {
        return documentAsMap.size();
    }

    @Override
    public boolean isEmpty() {
        return documentAsMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return documentAsMap.containsKey(key);
    }

    @Override
    public Object put(String key, Object value) {
    	if(key==null) { //add@byron
    		return null;
    	}
        return documentAsMap.put(key, value);
    }

    public void putIfNotNull(String key, Object value) {
        if (value != null) {
            put(key, value);
        }
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        documentAsMap.putAll(m);
    }

    @Override
    public Object remove(Object key) {
        return documentAsMap.remove(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Document clone() {
        return new Document((Map<String, Object>) documentAsMap.clone());
    }

    @Override
    public Set<String> keySet() {
        return documentAsMap.keySet();
    }

    @Override
    public Collection<Object> values() {
        return documentAsMap.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return documentAsMap.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof Document)) {
            return false;
        }
        List<String> keys = new ArrayList<>(keySet());
        List<String> otherKeys = new ArrayList<>(((Document) o).keySet());
        if (!keys.equals(otherKeys)) {
            return false;
        }
        return documentAsMap.equals(o);
    }

    @Override
    public int hashCode() {
        return documentAsMap.hashCode();
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean compactKey) {
        return toString(compactKey, "{", "}");
    }

    public String toString(boolean compactKey, String prefix, String suffix) {
        return documentAsMap.entrySet().stream()
            .map(entry -> writeKey(entry.getKey(), compactKey) + " " + Json.toJsonValue(entry.getValue(), compactKey, prefix, suffix))
            .collect(Collectors.joining(", ", prefix, suffix));
    }

    private String writeKey(String key, boolean compact) {
        if (compact) {
            return Json.escapeJson(key) + ":";
        } else {
            return "\"" + Json.escapeJson(key) + "\" :";
        }
    }

    public LinkedHashMap asMap() {
    	return this.documentAsMap;
    }
}
