package de.bwaldvogel.mongo.backend;

public class IndexKey {

    private final String key;
    private final boolean ascending;

    public IndexKey(String key, boolean ascending) {
        this.key = key;
        this.ascending = ascending;
    }

    public String getKey() {
        return key;
    }

    public boolean isAscending() {
        return ascending;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[key=" + key + " " + (ascending ? "ASC" : "DESC") + "]";
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexKey other = (IndexKey) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
    
}
