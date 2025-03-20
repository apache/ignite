package org.apache.ignite.console.agent.db;

import org.apache.ignite.console.agent.db.Dialect;
import org.apache.ignite.console.agent.db.IntrospectedTable;
import org.apache.ignite.console.agent.utils.DBMetadataUtils;
import org.beetl.core.Configuration;
import org.beetl.core.GroupTemplate;
import org.beetl.core.Template;
import org.beetl.core.resource.FileResourceLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.List;

/**
 * @author liuzh
 */
public class BeetlTemplate {
    public static final String root = BeetlTemplate.class.getResource("/").getPath() + "beetl";

    public static final FileResourceLoader resourceLoader = new FileResourceLoader(root, "UTF-8");

    public static Configuration cfg;

    static {
        try {
            cfg = Configuration.defaultConfiguration();
        } catch (IOException e) {
            cfg = null;
        }
    }

    public static final GroupTemplate gt = new GroupTemplate(resourceLoader, cfg);

    /**
     * 导出一个数据库的html文件
     *
     * @param tables
     * @param filePath
     * @param fileName
     * @return
     * @throws IOException
     */
    public static String exportDatabaseHtml(List<IntrospectedTable> tables, String filePath, String fileName) throws IOException {
        mkdirs(filePath);
        Template t = gt.getTemplate("datebase.html");
        t.binding("tables", tables);
        t.binding("fileName", fileName);
        String path = filePath + File.separator + fileName + ".html";
        render(t, path);
        return path;
    }

    /**
     * 导出一个Table的html文件
     *
     * @param table
     * @return
     * @throws IOException
     */
    public static String exportTableHtml(IntrospectedTable table, String filePath, String fileName) throws IOException {
        mkdirs(filePath);
        Template t = gt.getTemplate("table.html");
        t.binding("table", table);
        t.binding("fileName", table.getName());
        String path = filePath + File.separator + fileName + ".html";
        render(t, path);
        return path;
    }

    /**
     * 创建目录
     *
     * @param filePath
     */
    private static void mkdirs(String filePath){
        File file = new File(filePath);
        if (!file.isDirectory() || !file.exists()) {
            file.mkdirs();
        }
    }

    /**
     * 保存到指定位置
     *
     * @param t
     * @param path
     * @throws IOException
     */
    private static void render(Template t, String path) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(path), "UTF-8");

        t.renderTo(writer);

        writer.close();
    }
}
