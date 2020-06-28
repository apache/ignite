package de.bwaldvogel.mongo.backend;

public class IndexKey {

    private final String key;
    private final boolean ascending;
    private final boolean isText;
    
    public IndexKey(String key, boolean ascending) {
        this.key = key;
        this.ascending = ascending;
        this.isText = false;
    }

    public IndexKey(String key, boolean ascending,boolean isText) {
        this.key = key;
        this.ascending = ascending;
        this.isText = isText;
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

	public boolean isText() {
		return isText;
	}
}
