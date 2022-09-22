package de.kp.works.ignite.transform.fleet

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

object FleetFormats extends Enumeration {

  type FleetFormat = Value

  val RESULT: FleetFormats.Value = Value(1, "result.log")
  val STATUS: FleetFormats.Value = Value(2, "status.log")

}

object FleetFormatUtil {

  def fromFile(fileName:String):FleetFormats.Value = {

    val formats = FleetFormats.values.filter(format => {
      fileName.contains(format.toString)
    })

    if (formats.isEmpty) return null
    formats.head

  }
}