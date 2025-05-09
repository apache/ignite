package org.apache.ignite.console.agent.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IntrospectedTable extends IntrospectedBase {
    private String schema;
    private String catalog;
    protected List<IntrospectedColumn> primaryKeyColumns;
    protected List<IntrospectedColumn> baseColumns;
    protected List<IntrospectedIndex> uniqueIndexes;
    protected List<IntrospectedIndex> indexes;

	public IntrospectedTable() {
        super();
        primaryKeyColumns = new ArrayList<>();
        baseColumns = new ArrayList<>();
        uniqueIndexes = new ArrayList<>();
        indexes = new ArrayList<>();
    }

    public IntrospectedTable(String catalog, String schema, String name) {
        this();
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
    }

    public IntrospectedColumn getColumn(String columnName) {
        if (columnName == null) {
            return null;
        } else {
            for (IntrospectedColumn introspectedColumn : baseColumns) {
                if (introspectedColumn.isColumnNameDelimited()) {
                    if (introspectedColumn.getName().equals(
                            columnName)) {
                        return introspectedColumn;
                    }
                } else {
                    if (introspectedColumn.getName()
                            .equalsIgnoreCase(columnName)) {
                        return introspectedColumn;
                    }
                }
            }
            return null;
        }
    }

    public boolean hasJDBCDateColumns() {
        boolean rc = false;
        if (!rc) {
            for (IntrospectedColumn introspectedColumn : baseColumns) {
                if (introspectedColumn.isJDBCDateColumn()) {
                    rc = true;
                    break;
                }
            }
        }
        return rc;
    }

    public boolean hasJDBCTimeColumns() {
        boolean rc = false;
        if (!rc) {
            for (IntrospectedColumn introspectedColumn : baseColumns) {
                if (introspectedColumn.isJDBCTimeColumn()) {
                    rc = true;
                    break;
                }
            }
        }
        return rc;
    }

    public List<IntrospectedColumn> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public boolean hasPrimaryKeyColumns() {
        return primaryKeyColumns.size() > 0;
    }

    public List<IntrospectedColumn> getBaseColumns() {
        return baseColumns;
    }

    public List<IntrospectedColumn> getAllColumns() {
        return baseColumns;
    }

    public boolean hasBaseColumns() {
        return baseColumns.size() > 0;
    }

    public boolean hasAnyColumns() {
        return baseColumns.size() > 0;
    }

    public void addColumn(IntrospectedColumn introspectedColumn) {
        baseColumns.add(introspectedColumn);
        introspectedColumn.setIntrospectedTable(this);
    }

    public void addPrimaryKeyColumn(String columnName) {
        Iterator<IntrospectedColumn> iter = baseColumns.iterator();
        while (iter.hasNext()) {
            IntrospectedColumn introspectedColumn = iter.next();
            if (introspectedColumn.getName().equals(columnName)) {
                introspectedColumn.setPk(true);
                primaryKeyColumns.add(introspectedColumn);
                break;
            }
        }
    }
    
    /**
     * Select first shortest index.
     *
     * @param uniqueIdxs Unique indexes with columns.
     * @return Unique index that could be used instead of primary key.
     */
    protected IntrospectedIndex uniqueIndexAsPk() {
    	IntrospectedIndex uniqueIdxAsPk = null;

        for (IntrospectedIndex uniqueIdx : uniqueIndexes) {
            if (uniqueIdxAsPk == null || uniqueIdxAsPk.columns.size() > uniqueIdx.columns.size())
                uniqueIdxAsPk = uniqueIdx;
        }

        return uniqueIdxAsPk;
    }
    
    public List<IntrospectedIndex> getUniqueIndexes() {
    	return uniqueIndexes;
    }
    
    public List<IntrospectedIndex> getIndexes() {
    	return indexes;
    }
    
    public void setUniqueIndexes(List<IntrospectedIndex> uniqueIndexes) {
		this.uniqueIndexes = uniqueIndexes;
	}

	public void setIndexes(List<IntrospectedIndex> indexes) {
		this.indexes = indexes;
	}

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IntrospectedTable)) return false;

        IntrospectedTable that = (IntrospectedTable) o;

        if (catalog != null ? !catalog.equals(that.catalog) : that.catalog != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        result = 31 * result + (catalog != null ? catalog.hashCode() : 0);
        return result;
    }
}
