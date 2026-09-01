package com.stranger.domain;

import com.stranger.domain.GenTable;
import com.stranger.domain.GenTableColumn;

import java.util.List;

public class GenTableData {
    private GenTable genTables;

    private List<GenTableColumn> genTableColumns;

    public void setGenTables(GenTable genTables) {
        this.genTables = genTables;
    }

    public void setGenTableColumns(List<GenTableColumn> genTableColumns) {
        this.genTableColumns = genTableColumns;
    }

    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof com.stranger.domain.GenTableData))
            return false;
        com.stranger.domain.GenTableData other = (com.stranger.domain.GenTableData)o;
        if (!other.canEqual(this))
            return false;
        Object this$genTables = getGenTables(), other$genTables = other.getGenTables();
        if ((this$genTables == null) ? (other$genTables != null) : !this$genTables.equals(other$genTables))
            return false;
        List<GenTableColumn> this$genTableColumns = (List<GenTableColumn>)getGenTableColumns(), other$genTableColumns = (List<GenTableColumn>)other.getGenTableColumns();
        return !((this$genTableColumns == null) ? (other$genTableColumns != null) : !this$genTableColumns.equals(other$genTableColumns));
    }

    protected boolean canEqual(Object other) {
        return other instanceof com.stranger.domain.GenTableData;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $genTables = getGenTables();
        result = result * 59 + (($genTables == null) ? 43 : $genTables.hashCode());
        List<GenTableColumn> genTableColumns = (List<GenTableColumn>)getGenTableColumns();
        return result * 59 + ((genTableColumns == null) ? 43 : genTableColumns.hashCode());
    }

    public String toString() {
        return "GenTableData(genTables=" + getGenTables() + ", genTableColumns=" + getGenTableColumns() + ")";
    }

    public GenTable getGenTables() {
        return this.genTables;
    }

    public List<GenTableColumn> getGenTableColumns() {
        return this.genTableColumns;
    }
}
