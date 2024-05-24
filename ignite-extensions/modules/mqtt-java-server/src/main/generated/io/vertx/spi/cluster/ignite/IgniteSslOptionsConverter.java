package io.vertx.spi.cluster.ignite;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link io.vertx.spi.cluster.ignite.IgniteSslOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.spi.cluster.ignite.IgniteSslOptions} original class using Vert.x codegen.
 */
public class IgniteSslOptionsConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, IgniteSslOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "jksKeyCertOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setJksKeyCertOptions(new io.vertx.core.net.JksOptions((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "jksTrustOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setJksTrustOptions(new io.vertx.core.net.JksOptions((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "keyAlgorithm":
          if (member.getValue() instanceof String) {
            obj.setKeyAlgorithm((String)member.getValue());
          }
          break;
        case "keyStoreFilePath":
          if (member.getValue() instanceof String) {
            obj.setKeyStoreFilePath((String)member.getValue());
          }
          break;
        case "keyStorePassword":
          if (member.getValue() instanceof String) {
            obj.setKeyStorePassword((String)member.getValue());
          }
          break;
        case "keyStoreType":
          if (member.getValue() instanceof String) {
            obj.setKeyStoreType((String)member.getValue());
          }
          break;
        case "pemKeyCertOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setPemKeyCertOptions(new io.vertx.core.net.PemKeyCertOptions((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "pemTrustOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setPemTrustOptions(new io.vertx.core.net.PemTrustOptions((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "pfxKeyCertOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setPfxKeyCertOptions(new io.vertx.core.net.PfxOptions((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "pfxTrustOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setPfxTrustOptions(new io.vertx.core.net.PfxOptions((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "protocol":
          if (member.getValue() instanceof String) {
            obj.setProtocol((String)member.getValue());
          }
          break;
        case "trustAll":
          if (member.getValue() instanceof Boolean) {
            obj.setTrustAll((Boolean)member.getValue());
          }
          break;
        case "trustStoreFilePath":
          if (member.getValue() instanceof String) {
            obj.setTrustStoreFilePath((String)member.getValue());
          }
          break;
        case "trustStorePassword":
          if (member.getValue() instanceof String) {
            obj.setTrustStorePassword((String)member.getValue());
          }
          break;
        case "trustStoreType":
          if (member.getValue() instanceof String) {
            obj.setTrustStoreType((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(IgniteSslOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(IgniteSslOptions obj, java.util.Map<String, Object> json) {
    if (obj.getJksKeyCertOptions() != null) {
      json.put("jksKeyCertOptions", obj.getJksKeyCertOptions().toJson());
    }
    if (obj.getJksTrustOptions() != null) {
      json.put("jksTrustOptions", obj.getJksTrustOptions().toJson());
    }
    if (obj.getKeyAlgorithm() != null) {
      json.put("keyAlgorithm", obj.getKeyAlgorithm());
    }
    if (obj.getKeyStoreFilePath() != null) {
      json.put("keyStoreFilePath", obj.getKeyStoreFilePath());
    }
    if (obj.getKeyStorePassword() != null) {
      json.put("keyStorePassword", obj.getKeyStorePassword());
    }
    if (obj.getKeyStoreType() != null) {
      json.put("keyStoreType", obj.getKeyStoreType());
    }
    if (obj.getPemKeyCertOptions() != null) {
      json.put("pemKeyCertOptions", obj.getPemKeyCertOptions().toJson());
    }
    if (obj.getPemTrustOptions() != null) {
      json.put("pemTrustOptions", obj.getPemTrustOptions().toJson());
    }
    if (obj.getPfxKeyCertOptions() != null) {
      json.put("pfxKeyCertOptions", obj.getPfxKeyCertOptions().toJson());
    }
    if (obj.getPfxTrustOptions() != null) {
      json.put("pfxTrustOptions", obj.getPfxTrustOptions().toJson());
    }
    if (obj.getProtocol() != null) {
      json.put("protocol", obj.getProtocol());
    }
    json.put("trustAll", obj.isTrustAll());
    if (obj.getTrustStoreFilePath() != null) {
      json.put("trustStoreFilePath", obj.getTrustStoreFilePath());
    }
    if (obj.getTrustStorePassword() != null) {
      json.put("trustStorePassword", obj.getTrustStorePassword());
    }
    if (obj.getTrustStoreType() != null) {
      json.put("trustStoreType", obj.getTrustStoreType());
    }
  }
}
