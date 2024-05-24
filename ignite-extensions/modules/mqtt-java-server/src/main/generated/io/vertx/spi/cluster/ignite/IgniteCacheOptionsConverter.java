package io.vertx.spi.cluster.ignite;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link io.vertx.spi.cluster.ignite.IgniteCacheOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.spi.cluster.ignite.IgniteCacheOptions} original class using Vert.x codegen.
 */
public class IgniteCacheOptionsConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, IgniteCacheOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "atomicityMode":
          if (member.getValue() instanceof String) {
            obj.setAtomicityMode((String)member.getValue());
          }
          break;
        case "backups":
          if (member.getValue() instanceof Number) {
            obj.setBackups(((Number)member.getValue()).intValue());
          }
          break;
        case "cacheMode":
          if (member.getValue() instanceof String) {
            obj.setCacheMode((String)member.getValue());
          }
          break;
        case "copyOnRead":
          if (member.getValue() instanceof Boolean) {
            obj.setCopyOnRead((Boolean)member.getValue());
          }
          break;
        case "eagerTtl":
          if (member.getValue() instanceof Boolean) {
            obj.setEagerTtl((Boolean)member.getValue());
          }
          break;
        case "encryptionEnabled":
          if (member.getValue() instanceof Boolean) {
            obj.setEncryptionEnabled((Boolean)member.getValue());
          }
          break;
        case "eventsDisabled":
          if (member.getValue() instanceof Boolean) {
            obj.setEventsDisabled((Boolean)member.getValue());
          }
          break;
        case "expiryPolicy":
          if (member.getValue() instanceof JsonObject) {
            obj.setExpiryPolicy(((JsonObject)member.getValue()).copy());
          }
          break;
        case "groupName":
          if (member.getValue() instanceof String) {
            obj.setGroupName((String)member.getValue());
          }
          break;
        case "invalidate":
          if (member.getValue() instanceof Boolean) {
            obj.setInvalidate((Boolean)member.getValue());
          }
          break;
        case "maxConcurrentAsyncOperations":
          if (member.getValue() instanceof Number) {
            obj.setMaxConcurrentAsyncOperations(((Number)member.getValue()).intValue());
          }
          break;
        case "maxQueryInteratorsCount":
          if (member.getValue() instanceof Number) {
            obj.setMaxQueryInteratorsCount(((Number)member.getValue()).intValue());
          }
          break;
        case "metricsEnabled":
          if (member.getValue() instanceof Boolean) {
            obj.setMetricsEnabled((Boolean)member.getValue());
          }
          break;
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
        case "onheapCacheEnabled":
          if (member.getValue() instanceof Boolean) {
            obj.setOnheapCacheEnabled((Boolean)member.getValue());
          }
          break;
        case "partitionLossPolicy":
          if (member.getValue() instanceof String) {
            obj.setPartitionLossPolicy((String)member.getValue());
          }
          break;
        case "readFromBackup":
          if (member.getValue() instanceof Boolean) {
            obj.setReadFromBackup((Boolean)member.getValue());
          }
          break;
        case "rebalanceDelay":
          if (member.getValue() instanceof Number) {
            obj.setRebalanceDelay(((Number)member.getValue()).longValue());
          }
          break;
        case "rebalanceMode":
          if (member.getValue() instanceof String) {
            obj.setRebalanceMode((String)member.getValue());
          }
          break;
        case "rebalanceOrder":
          if (member.getValue() instanceof Number) {
            obj.setRebalanceOrder(((Number)member.getValue()).intValue());
          }
          break;
        case "writeSynchronizationMode":
          if (member.getValue() instanceof String) {
            obj.setWriteSynchronizationMode((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(IgniteCacheOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(IgniteCacheOptions obj, java.util.Map<String, Object> json) {
    if (obj.getAtomicityMode() != null) {
      json.put("atomicityMode", obj.getAtomicityMode());
    }
    json.put("backups", obj.getBackups());
    if (obj.getCacheMode() != null) {
      json.put("cacheMode", obj.getCacheMode());
    }
    json.put("copyOnRead", obj.isCopyOnRead());
    json.put("eagerTtl", obj.isEagerTtl());
    json.put("encryptionEnabled", obj.isEncryptionEnabled());
    json.put("eventsDisabled", obj.isEventsDisabled());
    if (obj.getExpiryPolicy() != null) {
      json.put("expiryPolicy", obj.getExpiryPolicy());
    }
    if (obj.getGroupName() != null) {
      json.put("groupName", obj.getGroupName());
    }
    json.put("invalidate", obj.isInvalidate());
    json.put("maxConcurrentAsyncOperations", obj.getMaxConcurrentAsyncOperations());
    json.put("maxQueryInteratorsCount", obj.getMaxQueryInteratorsCount());
    json.put("metricsEnabled", obj.isMetricsEnabled());
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
    json.put("onheapCacheEnabled", obj.isOnheapCacheEnabled());
    if (obj.getPartitionLossPolicy() != null) {
      json.put("partitionLossPolicy", obj.getPartitionLossPolicy());
    }
    json.put("readFromBackup", obj.isReadFromBackup());
    json.put("rebalanceDelay", obj.getRebalanceDelay());
    if (obj.getRebalanceMode() != null) {
      json.put("rebalanceMode", obj.getRebalanceMode());
    }
    json.put("rebalanceOrder", obj.getRebalanceOrder());
    if (obj.getWriteSynchronizationMode() != null) {
      json.put("writeSynchronizationMode", obj.getWriteSynchronizationMode());
    }
  }
}
