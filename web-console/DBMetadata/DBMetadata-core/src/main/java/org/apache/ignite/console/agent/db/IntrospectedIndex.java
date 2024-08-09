package org.apache.ignite.console.agent.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IntrospectedIndex extends IntrospectedBase {
    private String type;
    protected Map<String,String> columns;

    public IntrospectedIndex() {
        super();        
        columns = new HashMap<>();
    }

    public IntrospectedIndex(String name,String type) {
        this();       
        this.type = type;
        this.name = name;       
    }

    public Map<String,String> getColumns() {
        return columns;
    }   

    public void addColumn(String name, String asc) {
        columns.put(name,asc);        
    }
    
    public void addColumns(Collection<String> names, String asc) {
    	for(String name: names)
    		columns.put(name,asc);        
    }

    public String getType() {
        return type;
    }


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columns == null) ? 0 : columns.hashCode());
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
		IntrospectedIndex other = (IntrospectedIndex) obj;
		if (columns == null) {
			if (other.columns != null)
				return false;
		} else if (!columns.equals(other.columns))
			return false;
		return true;
	}
    
}
