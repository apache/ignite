package org.apache.ignite.internal.processors.rest.handlers.redis.list;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class ScoredItem<V> implements java.lang.Comparable<ScoredItem<V>>,Cloneable{
	/** */
    private static final long serialVersionUID = 1L;
    

    private final V value;

    /** */
    private double score;
    

    /**
     * Fully initializes this tuple.
     *
     * @param val1 First value.
     * @param val2 Second value.
     */
    public ScoredItem(V value, double score) {
        this.value = value;
        this.score = score;
    }    
   
    public V getValue() {
        return value;
    }

    public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
	/** Converts IEEE 754 representation of a double to sortable order (or back to the original) */
	public long scoreToSortableLong() {
		long bits = Double.doubleToLongBits(score);		
		return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		ScoredItem other = (ScoredItem) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public int compareTo(ScoredItem<V> o) {
		return Double.compare(this.score,o.score);
	}

}
