/*
 * Copyright 2022 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.vertx.spi.cluster.ignite.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.ClusterSerializable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Serialization/deserialization utils. Provides support of {@link ClusterSerializable} interface.
 *
 * @author Andrey Gura
 */
public class ClusterSerializationUtils {

  /**
   * Serializes and wraps to {@link ClusterSerializableValue} given object if it implements
   * {@link ClusterSerializable} interface, otherwise returns source value.
   *
   * @param obj Object.
   * @return {@link ClusterSerializableValue} instance as serialized form of passed object if it implements
   * {@link ClusterSerializable} interface, otherwise passed object itself.
   */
  public static <T> T marshal(T obj) {
    if (obj instanceof ClusterSerializable) {
      return (T) marshal0((ClusterSerializable) obj);
    } else {
      return obj;
    }
  }

  /**
   * Unwraps and deserializes {@link ClusterSerializableValue} or returns source value.
   *
   * @param obj Object.
   * @return Deserialized {@link ClusterSerializable} value or source value.
   */
  public static <T> T unmarshal(T obj) {
    if (obj instanceof ClusterSerializableValue) {
      return (T) unmarshal0((ClusterSerializableValue) obj);
    } else {
      return obj;
    }
  }

  private static ClusterSerializableValue marshal0(ClusterSerializable obj) {
    Buffer buffer = Buffer.buffer();
    obj.writeToBuffer(buffer);
    return new ClusterSerializableValue(obj.getClass().getName(), buffer.getBytes());
  }

  private static ClusterSerializable unmarshal0(ClusterSerializableValue value) {
    try {
      Class<?> cls = Thread.currentThread().getContextClassLoader().loadClass(value.getClassName());
      ClusterSerializable obj = (ClusterSerializable) cls.getDeclaredConstructor().newInstance();
      obj.readFromBuffer(0, Buffer.buffer(value.getData()));
      return obj;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to load class " + value.getClassName(), e);
    }
  }

  /**
   * Wrapper for serialized {@link ClusterSerializable}.
   */
  public static class ClusterSerializableValue {
    private final String clsName;
    private final byte[] data;

    public ClusterSerializableValue(String clsName, byte[] data) {
      this.clsName = clsName;
      this.data = data;
    }

    public String getClassName() {
      return clsName;
    }

    public byte[] getData() {
      return data;
    }

    @Override
    public boolean equals(Object that) {
      if (this == that)
        return true;
      if (that == null || getClass() != that.getClass())
        return false;
      ClusterSerializableValue value = (ClusterSerializableValue)that;
      return Objects.equals(clsName, value.clsName) &&
        Arrays.equals(data, value.data);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(clsName);
      result = 31 * result + Arrays.hashCode(data);
      return result;
    }
  }
}
