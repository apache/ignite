package com.stranger.mapper;

import com.stranger.domain.GenTableColumn;

import java.util.List;

public interface GenTableColumnMapper{
    List<GenTableColumn> selectDbTableColumnsByName(String paramString);
}
