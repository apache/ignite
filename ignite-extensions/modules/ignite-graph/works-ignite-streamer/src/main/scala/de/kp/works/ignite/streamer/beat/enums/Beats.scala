package de.kp.works.ignite.streamer.beat.enums

/**
 * Copyright (c) 2020 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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

object Beats extends Enumeration {

  type Beat = Value

  val FIWARE: Beats.Value  = Value(1, "fiware")
  val FLEET: Beats.Value   = Value(2, "fleet")
  val OPCUA: Beats.Value   = Value(3, "opcua")
  val OPENCTI: Beats.Value = Value(4, "opencti")
  val THINGS: Beats.Value  = Value(5, "things")
  val TLS: Beats.Value     = Value(6, "osquery")
  val ZEEK: Beats.Value    = Value(7, "zeek")

}