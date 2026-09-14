package com.stranger.mapper;

import com.stranger.domain.GenTableColumn;

import java.util.List;
import java.util.Map;

public interface GenTableColumnMapper{
    List<GenTableColumn> selectDbTableColumnsByName(String tableName, Map<String,Object> context);
}
