package com.stranger.mapper.impl;

import com.stranger.domain.GenTable;
import com.stranger.mapper.GenMapper;

import java.util.List;
import java.util.Map;

public class GenMapperImpl implements GenMapper {
    @Override
    public List<GenTable> selectDbTableList(Map<String,Object> context) {
        List<String> models = (List)context.get("models");
        return null;
    }
}
