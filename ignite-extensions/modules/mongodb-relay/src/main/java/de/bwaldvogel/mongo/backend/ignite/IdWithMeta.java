package de.bwaldvogel.mongo.backend.ignite;

import de.bwaldvogel.mongo.bson.Document;

public final class IdWithMeta {

    final Object key;
    final boolean ascending;
    final Document meta;
    
    Object indexValue;

    public IdWithMeta(Object key, boolean ascending) {
        this.key = key;
        this.ascending = ascending;
        this.meta = null;
    }

    public IdWithMeta(Object key, boolean ascending,Document textOptions) {
        this.key = key;
        this.ascending = ascending;
        this.meta = textOptions;
    }

    public Object getKey() {
        return key;
    }

    public boolean isAscending() {
        return ascending;
    }

    public IdWithMeta indexValue(Object indexValue) {
    	this.indexValue = indexValue;
    	return this;
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[key=" + key + " " + (ascending ? "ASC" : "DESC") + "]";
    }
    
    public Document meta() {
        return meta;
    }

	@Override
	public int hashCode() {		
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass()) {
			if(key.equals(obj)) {
				return true;
			}
			return false;
		}
		IdWithMeta other = (IdWithMeta) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
    
    
}