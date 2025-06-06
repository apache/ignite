package org.apache.ignite.console.agent.db;


import com.stranger.common.config.VelocityInitializer;
import org.apache.commons.io.IOUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

/**
 * @author liuzh
 */
public class HtmlTableTemplate {
    public static final String root = HtmlTableTemplate.class.getResource("/").getPath() + "html";

    private static final Logger log = LoggerFactory.getLogger(HtmlTableTemplate.class);

    static {
        VelocityInitializer.initVelocity();
    }

    /**
     * 导出一个数据库的html文件
     *
     * @param tables
     * @param filePath
     * @param fileName
     * @return
     * @throws IOException
     */
    public String exportDatabaseHtml(List<IntrospectedTable> tables, String filePath, String fileName) throws IOException {
        mkdirs(filePath);

        VelocityContext context = new VelocityContext();

        context.put("tables", tables);
        context.put("fileName", fileName);
        String path = filePath + File.separator + fileName + ".html";
        List<String> templates = List.of("vm/ui/datebase.html");
        for (String template : templates) {

            try {
                StringWriter sw = new StringWriter();
                Template tpl = Velocity.getTemplate(template, "UTF-8");
                tpl.merge((Context) context, sw);
                render(sw, path);
            } catch (IOException e) {
                log.error("渲染模板失败，模板名：" + template, e);
            }
        }
        return path;
    }

    /**
     * 导出一个Table的html文件
     *
     * @param table
     * @return
     * @throws IOException
     */
    public String exportTableHtml(IntrospectedTable table, String filePath, String fileName) throws IOException {
        mkdirs(filePath);
        VelocityContext context = new VelocityContext();

        context.put("table", table);
        context.put("fileName", fileName);
        String path = filePath + File.separator + fileName + ".html";
        List<String> templates = List.of("vm/ui/table.html");
        for (String template : templates) {

            try {
                StringWriter sw = new StringWriter();
                Template tpl = Velocity.getTemplate(template, "UTF-8");
                tpl.merge((Context) context, sw);
                render(sw, path);
            } catch (IOException e) {
                log.error("渲染模板失败，模板名：" + template, e);
            }
        }
        return path;
    }

    /**
     * 创建目录
     *
     * @param filePath
     */
    static void mkdirs(String filePath){
        File file = new File(filePath);
        if (!file.isDirectory() || !file.exists()) {
            file.mkdirs();
        }
    }

    /**
     * 保存到指定位置
     *
     * @param sw
     * @param path
     * @throws IOException
     */
    static void render(StringWriter sw, String path) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(path), "UTF-8");

        IOUtils.write(sw.toString(), writer);
        IOUtils.closeQuietly(sw);

        writer.close();
    }
}
