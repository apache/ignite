package de.kp.works.ignite.transform.opencti
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

object CTISchema {

  def sighting():StructType = {

    val fields = Array(
      /*
       * The action specified by OpenCTI how to process
       * this sighting
       */
      StructField("action",    StringType, nullable = false),
      StructField("operation", StringType, nullable = false),
      /*
       * Each sighting is identified by `id` and `type`
       */
      StructField("entity_id",   StringType, nullable = false),
      StructField("entity_type", StringType, nullable = false),
      /*
        * Each sighting is specified with a list of attributes,
        * and each attribute is described by name, type, value
        * and metadata.
        *
        * For a common representation, all attribute values are
        * represented by its STRING values
        */
      StructField("attr_name",  StringType, nullable = false),
      StructField("attr_type",  StringType, nullable = false),
      StructField("attr_value", StringType, nullable = false),

      StructField("sighting_of_ref",    StringType, nullable = false),
      StructField("where_sighted_refs", StringType, nullable = false)

    )

    StructType(fields)

  }

  def stix_object():StructType = {

    val fields = Array(
      /*
       * The action specified by OpenCTI how to process
       * this STIX object
       */
      StructField("action",    StringType, nullable = false),
      StructField("operation", StringType, nullable = false),
      /*
       * Each STIX object is identified by `id` and `type`
       */
      StructField("entity_id",   StringType, nullable = false),
      StructField("entity_type", StringType, nullable = false),
      /*
       * Each object is specified with a list of attributes,
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
       * Defined hash values
       */
      StructField("MD5",      StringType, nullable = true),
      StructField("SHA_1",    StringType, nullable = true),
      StructField("SHA_256",  StringType, nullable = true),
      StructField("SHA_512",  StringType, nullable = true),
      StructField("SHA3_256", StringType, nullable = true),
      StructField("SHA3_512", StringType, nullable = true),
      StructField("SSDEEP",   StringType, nullable = true),
      /*
       * References the identity that created this object;
       * In order to harmonize create & update requests,
       * this filed is specified as an Array
       */
      StructField("created_by_ref", ArrayType(StringType, containsNull = false), nullable = true),
      /*
       * The references to external objects; this is
       * specified as a set of serialized objects
       */
      StructField("external_references", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("kill_chain_phases",   ArrayType(StringType, containsNull = false), nullable = true),
      StructField("labels",              ArrayType(StringType, containsNull = false), nullable = true),
      StructField("object_marking_refs", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("object_refs",         ArrayType(StringType, containsNull = false), nullable = true)

    )

    StructType(fields)

  }
}
