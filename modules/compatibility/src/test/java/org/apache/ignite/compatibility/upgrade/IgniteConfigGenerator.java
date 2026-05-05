package org.apache.ignite.compatibility.upgrade;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.apache.ignite.configuration.IgniteConfiguration;

/** */
public class IgniteConfigGenerator {
    /** */
    private final Configuration cfg;

    /** */
    private final Transformer transformer;

    /** */
    public IgniteConfigGenerator() {
        this.cfg = getFreemarketConfiguration();
        this.transformer = getXmlTransformer();
    }

    /** */
    private Configuration getFreemarketConfiguration() {
        Configuration cfg0 = new Configuration(Configuration.VERSION_2_3_34);

        cfg0.setClassForTemplateLoading(this.getClass(), "/templates");

        DefaultObjectWrapper wrapper = new DefaultObjectWrapper(Configuration.VERSION_2_3_34);
        wrapper.setExposeFields(true);

        cfg0.setObjectWrapper(wrapper);
        cfg0.setDefaultEncoding("UTF-8");
        cfg0.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        return cfg0;
    }

    /** */
    private Transformer getXmlTransformer() {
        Transformer transformer0;

        try {
            TransformerFactory factory = TransformerFactory.newInstance();

            factory.setAttribute("indent-number", 4);

            transformer0 = factory.newTransformer();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize XML transformer", e);
        }

        transformer0.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer0.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer0.setOutputProperty(OutputKeys.METHOD, "xml");

        return transformer0;
    }

    /**
     * Generates Ignite configuration at a specific, deterministic path.
     *
     * @param igniteCfg Ignite configuration object.
     * @param logDir Directory for logs.
     * @param cfgPath The base directory path where the "config.xml" should be created.
     * @return The absolute {@link Path} to the generated "config.xml" file.
     */
    public Path generateIgniteConfigurationXml(
        IgniteConfiguration igniteCfg,
        String logDir,
        String cfgPath
    ) throws Exception {
        Path targetPath = Paths.get(cfgPath + "/ignite-config.xml").toAbsolutePath();

        Files.createDirectories(targetPath.getParent());

        Path logXmlPath = generateLog4j2Xml(logDir, cfgPath);

        Map<String, Object> model = new HashMap<>();
        model.put("config", igniteCfg);
        model.put("logConfigPath", logXmlPath.toAbsolutePath().toString());

        Template igniteTemp = cfg.getTemplate("ignite.ftl");

        StringWriter sw = new StringWriter();
        igniteTemp.process(model, sw);
        String flatXml = sw.toString().replaceAll("(?s)>\\s+<", "><").trim();

        try (Writer out = Files.newBufferedWriter(targetPath)) {
            transformer.transform(
                new StreamSource(new StringReader(flatXml)),
                new StreamResult(out)
            );
        }

        return targetPath;
    }

    /** */
    public Path generateLog4j2Xml(String logDir, String cfgPath) throws Exception {
        Path logDirPath = Paths.get(logDir).toAbsolutePath();
        Path cfgDirPath = Paths.get(cfgPath).toAbsolutePath();

        Path cfgFilePath = cfgDirPath.resolve("log4j2.xml");

        Map<String, Object> logModel = new HashMap<>();
        logModel.put("logDir", logDirPath.toString());

        Template logTemp = cfg.getTemplate("log4j2.ftl");

        Files.createDirectories(logDirPath);

        try (Writer out = Files.newBufferedWriter(cfgFilePath)) {
            logTemp.process(logModel, out);
        }

        return cfgFilePath;
    }
}
