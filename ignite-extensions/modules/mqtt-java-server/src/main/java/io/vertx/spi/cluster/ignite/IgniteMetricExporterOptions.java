/*
 * Copyright 2021 Red Hat, Inc.
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
import org.apache.ignite.spi.metric.MetricExporterSpi;

/**
 * @author Markus Spika
 */
@DataObject(generateConverter = true)
public class IgniteMetricExporterOptions {

  private MetricExporterSpi customSpi;

  /**
   * Default Constructor
   */
  public IgniteMetricExporterOptions() {
  }

  /**
   * Copy Constructor
   */
  public IgniteMetricExporterOptions(final IgniteMetricExporterOptions options) {
    this.customSpi = options.customSpi;
  }

  /**
   * Constructor from JSON
   *
   * @param options the JSON
   */
  public IgniteMetricExporterOptions(JsonObject options) {
    this();
    IgniteMetricExporterOptionsConverter.fromJson(options, this);
  }

  @GenIgnore
  public MetricExporterSpi getCustomSpi() {
    return customSpi;
  }

  /**
   * Sets a custom MetricExporterSpi implementation.
   *
   * @param metricExporterSpi to set.
   * @return reference to this, for fluency
   */
  @GenIgnore
  public IgniteMetricExporterOptions setCustomSpi(MetricExporterSpi metricExporterSpi) {
    this.customSpi = metricExporterSpi;
    return this;
  }

  /**
   * Convert to JSON
   *
   * @return the JSON
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    IgniteMetricExporterOptionsConverter.toJson(this, json);
    return json;
  }
}
