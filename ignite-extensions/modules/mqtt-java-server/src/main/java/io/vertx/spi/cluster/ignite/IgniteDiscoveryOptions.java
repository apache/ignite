/*
 * Copyright 2020 Red Hat, Inc.
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
package io.vertx.spi.cluster.ignite;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.spi.discovery.DiscoverySpi;

/**
 * @author Lukas Prettenthaler
 */
@DataObject(generateConverter = true)
public class IgniteDiscoveryOptions {
  private String type;
  private JsonObject properties;
  private DiscoverySpi customSpi;

  /**
   * Default constructor
   */
  public IgniteDiscoveryOptions() {
    type = "TcpDiscoveryMulticastIpFinder";
    properties = new JsonObject();
  }

  /**
   * Copy constructor
   *
   * @param options the one to copy
   */
  public IgniteDiscoveryOptions(IgniteDiscoveryOptions options) {
    this.type = options.type;
  }

  /**
   * Constructor from JSON
   *
   * @param options the JSON
   */
  public IgniteDiscoveryOptions(JsonObject options) {
    this();
    IgniteDiscoveryOptionsConverter.fromJson(options, this);
  }

  /**
   * Get the discovery implementation type.
   *
   * @return Type of the implementation.
   */
  public String getType() {
    return type;
  }

  /**
   * Sets the discovery implementation type.
   * Defaults to TcpDiscoveryMulticastIpFinder
   *
   * @param type Implemenation type.
   * @return reference to this, for fluency
   */
  public IgniteDiscoveryOptions setType(String type) {
    this.type = type;
    return this;
  }

  /**
   * Get the discovery implementation properties.
   *
   * @return Properties of the discovery implementation.
   */
  public JsonObject getProperties() {
    return properties;
  }

  /**
   * Sets the properties used to configure the discovery implementation.
   *
   * @param properties Properties for the discovery implementation.
   * @return reference to this, for fluency
   */
  public IgniteDiscoveryOptions setProperties(JsonObject properties) {
    this.properties = properties;
    return this;
  }

  /**
   * Get the custom DiscoverySpi instance.
   *
   * @return DiscoverySpi.
   */
  @GenIgnore
  public DiscoverySpi getCustomSpi() {
    return customSpi;
  }

  /**
   * Sets a custom initialized DiscoverySpi. When a custom Spi is set all other properties are ignored.
   *
   * @param discoverySpi DiscoverySpi implementation.
   * @return reference to this, for fluency
   */
  @GenIgnore
  public IgniteDiscoveryOptions setCustomSpi(DiscoverySpi discoverySpi) {
    this.customSpi = discoverySpi;
    return this;
  }

  /**
   * Convert to JSON
   *
   * @return the JSON
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    IgniteDiscoveryOptionsConverter.toJson(this, json);
    return json;
  }
}
