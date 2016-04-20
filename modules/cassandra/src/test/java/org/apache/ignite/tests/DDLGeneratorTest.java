package org.apache.ignite.tests;

import org.apache.ignite.cache.store.cassandra.utils.DDLGenerator;
import org.junit.Test;

import java.net.URL;

public class DDLGeneratorTest {
    @Test
    @SuppressWarnings("unchecked")
    public void generatorTest() {
        ClassLoader clsLdr = DDLGeneratorTest.class.getClassLoader();

        URL url1 = clsLdr.getResource("org/apache/ignite/tests/persistence/primitive/persistence-settings-1.xml");
        String file1 = url1.getFile();

        URL url2 = clsLdr.getResource("org/apache/ignite/tests/persistence/pojo/persistence-settings-3.xml");
        String file2 = url2.getFile();

        DDLGenerator.main(new String[]{file1, file2});
    }

}
