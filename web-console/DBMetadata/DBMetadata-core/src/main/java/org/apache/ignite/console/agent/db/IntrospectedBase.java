package org.apache.ignite.console.agent.db;

import org.apache.ignite.console.agent.utils.SqlStringUtils;

public class IntrospectedBase {
    protected String name;
    protected String remarks;

    /**
     * 根据条件进行过滤
     *
     * @param searchText 搜索名称
     * @param searchComment 搜索注释
     * @param matchType
     * @param caseSensitive
     * @return
     */
    public boolean filter(String searchText, String searchComment, MatchType matchType, boolean caseSensitive) {
    	
        if (SqlStringUtils.isNotEmpty(searchText)) {        	
        	
            if (matchType == MatchType.EQUALS) {
                if (caseSensitive) {
                    if (!getName().equals(searchText)) {
                        return false;
                    }
                } else {
                    if (!getName().equalsIgnoreCase(searchText)) {
                        return false;
                    }
                }
            } 
            else if (matchType == MatchType.STARTS) {
                if (caseSensitive) {
                    if (!getName().startsWith(searchText)) {
                        return false;
                    }
                } else {
                    if (!getName().toLowerCase().startsWith(searchText.toLowerCase())) {
                        return false;
                    }
                }
            } 
            else {
                if (caseSensitive) {
                    if (getName().indexOf(searchText) == -1) {
                        return false;
                    }
                } else {
                    if (getName().toLowerCase().indexOf(searchText.toLowerCase()) == -1) {
                        return false;
                    }
                }
            }
        }
        
        if (SqlStringUtils.isNotEmpty(searchComment)) {
            if (matchType == MatchType.EQUALS) {
                if (caseSensitive) {
                    if (getRemarks() == null || !getRemarks().equals(searchComment)) {
                        return false;
                    }
                } else {
                    if (getRemarks() == null || !getRemarks().equalsIgnoreCase(searchComment)) {
                        return false;
                    }
                }
            }
            else if (matchType == MatchType.STARTS) {
                if (caseSensitive) {
                    if (getRemarks() == null || !getRemarks().startsWith(searchComment)) {
                        return false;
                    }
                } else {
                    if (getRemarks() == null || !getRemarks().toLowerCase().startsWith(searchComment.toLowerCase())) {
                        return false;
                    }
                }
            } 
            else {
                if (caseSensitive) {
                    if (getRemarks() == null || getRemarks().indexOf(searchComment) == -1) {
                        return false;
                    }
                } else {
                    if (getRemarks() == null || getRemarks().toLowerCase().indexOf(searchComment.toLowerCase()) == -1) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }
}
