package de.kp.works.ignite.streamer.zeek
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

object ZeekMapper {

  val mapping: Map[String, String] = Map(
    /*
     * Mapping specification of connection
     * related Zeek fields
     */
    "id.orig_h" -> "source_ip",
    "id.orig_p" -> "source_port",
    "id.resp_h" -> "destination_ip",
    "id.resp_p" -> "destination_port",
    "orig_bytes" -> "source_bytes",
    "resp_bytes" -> "destination_bytes",
    "local_orig" -> "source_local",
    "local_resp" -> "destination_local",
    "orig_pkts" -> "source_pkts",
    "orig_ip_bytes" -> "source_ip_bytes",
    "resp_pkts" -> "destination_pkts",
    "resp_ip_bytes" -> "destination_ip_bytes",
    "orig_l2_addr" -> "source_l2_addr",
    "resp_l2_addr" -> "destination_l2_addr"

  )
}
