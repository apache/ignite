package de.kp.works.ignite.transform.fiware

/*
 * Copyright (c) 2020 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.apache.spark.sql.types._

object FiwareSchema {

  def schema():StructType = {

    val fields = Array(
      /*
       * Each Fiware notification sent by the Context Broker
       * contains a reference to the originating subscription
       */
      StructField("subscription", StringType, nullable = false),
      /*
       * Each Fiware notification references a service and a
       * servicePath parameter
       */
      StructField("service",      StringType, nullable = false),
      StructField("service_path", StringType, nullable = false),
      /*
       * Each Fiware notification references a list of entities
       * and every entity is identified by `id` and `type`
       */
      StructField("entity_id",   StringType, nullable = false),
      StructField("entity_type", StringType, nullable = false),
      /*
       * Each entity is specified with a list of attributes,
       * and each attribute is described by name, type, value
       * and metadata.
       *
       * For a common representation, all attribute values are
       * represented by its STRING values
       */
      StructField("attr_name",  StringType, nullable = false),
      StructField("attr_type",  StringType, nullable = false),
      StructField("attr_value", StringType, nullable = false),
      /*
       * Metadata is represented in its JSON serialized version,
       * and it is left to the application to interpret this
       * field correctly
       */
      StructField("metadata", StringType, nullable = false),
      StructField("context", ArrayType(StringType, containsNull = false), nullable = true)

    )

    StructType(fields)

  }

}
