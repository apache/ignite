package com.stranger.service.impl;

import com.stranger.common.config.GenConfig;
import com.stranger.common.config.VelocityInitializer;
import com.stranger.common.utils.GenUtils;
import com.stranger.common.utils.StringUtil;
import com.stranger.common.utils.VelocityUtils;
import com.stranger.domain.GenTable;
import com.stranger.domain.GenTableColumn;
import com.stranger.domain.GenTableData;
import com.stranger.mapper.GenMapper;
import com.stranger.mapper.GenTableColumnMapper;
import com.stranger.service.GenService;
import org.apache.commons.io.IOUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class GenServiceImpl implements GenService {
    private static final Logger log = LoggerFactory.getLogger(com.stranger.service.GenService.class);

    @Autowired(required=true)
    private GenTableColumnMapper genTableColumnMapper;

    @Autowired(required=true)
    private GenMapper genTableMapper;

    @Override
    public List<GenTable> selectDbTableList() {
        return this.genTableMapper.selectDbTableList();
    }

    @Override
    public List<GenTableData> buildTableInfo(List<GenTable> genTables,GenConfig genConfig) {
        String author = genConfig.author;
        List<GenTableData> objects = new ArrayList<>();
        try {
            for (GenTable table : genTables) {
                GenTableData genTableData = new GenTableData();
                ArrayList<GenTableColumn> tableColumns = new ArrayList<>();
                String tableName = table.getTableName();
                GenUtils.initTable(table, author,genConfig);
                genTableData.setGenTables(table);
                List<GenTableColumn> genTableColumns = genTableColumnMapper.selectDbTableColumnsByName(tableName);
                for (GenTableColumn column : genTableColumns) {
                    GenUtils.initColumnField(column, table);
                    tableColumns.add(column);
                }
                genTableData.setGenTableColumns(tableColumns);
                objects.add(genTableData);
            }
        } catch (Exception e) {
            throw new RuntimeException("渲染失败：" + e.getMessage());
        }
        return objects;
    }

    @Override
    public void generatorCode(List<GenTableData> genTableData, ZipOutputStream zip) {

        for (GenTableData genTableDatum : genTableData) {
            GenTable genTable = genTableDatum.getGenTables();
            genTable.setColumns(genTableDatum.getGenTableColumns());
            setPkColumn(genTable);
            VelocityInitializer.initVelocity();
            VelocityContext context = VelocityUtils.prepareContext(genTable);
            List<String> templates = VelocityUtils.getTemplateList(genTable.getTplCategory());
            for (String template : templates) {
                StringWriter sw = new StringWriter();
                Template tpl = Velocity.getTemplate(template, "UTF-8");
                tpl.merge((Context) context, sw);
                try {
                    zip.putNextEntry(new ZipEntry(VelocityUtils.getFileName(template, genTable)));
                    IOUtils.write(sw.toString(), zip, "UTF-8");
                    IOUtils.closeQuietly(sw);
                    zip.flush();
                    zip.closeEntry();
                } catch (IOException e) {
                    log.error("渲染模板失败，表名：" + genTable.getTableName(), e);
                }
            }
        }
    }

    @Override
    public void setPkColumn(GenTable table) {
        for (GenTableColumn column : table.getColumns()) {
            if (column.isPk()) {
                table.setPkColumn(column);
                break;
            }
        }
        if (StringUtil.isNull(table.getPkColumn()))
            table.setPkColumn(table.getColumns().get(0));
        if ("sub".equals(table.getTplCategory())) {
            for (GenTableColumn column : table.getSubTable().getColumns()) {
                if (column.isPk()) {
                    table.getSubTable().setPkColumn(column);
                    break;
                }
            }
            if (StringUtil.isNull(table.getSubTable().getPkColumn()))
                table.getSubTable().setPkColumn(table.getSubTable().getColumns().get(0));
        }

    }
}
