package com.stranger.mapper.impl;

import com.stranger.domain.GenTableColumn;
import com.stranger.mapper.GenTableColumnMapper;

import java.util.List;
import java.util.Map;

public class GenTableColumnMapperImpl implements GenTableColumnMapper {
    @Override
    public List<GenTableColumn> selectDbTableColumnsByName(String tableName, Map<String,Object> context) {
        return null;
    }
}
