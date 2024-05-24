package io.vertx.spi.cluster.ignite.util;

import io.vertx.core.VertxException;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class ClasspathHelperTest {
  @Test
  public void loadXml() {
    IgniteConfiguration config = ConfigHelper.lookupXmlConfiguration(this.getClass(), "ignite-test.xml");
    assertNotNull(config);
  }

  @Test(expected = VertxException.class)
  public void loadNotExistingXml() {
    ConfigHelper.lookupXmlConfiguration(this.getClass(), "does-not-exist.xml");
  }

  @Test
  public void loadJson() {
    JsonObject config = ConfigHelper.lookupJsonConfiguration(this.getClass(), "ignite.json");
    assertNotNull(config);
    assertFalse(config.isEmpty());
  }

  @Test(expected = VertxException.class)
  public void loadNotExistingJson() {
    ConfigHelper.lookupJsonConfiguration(this.getClass(), "does-not-exist.json");
  }
}
