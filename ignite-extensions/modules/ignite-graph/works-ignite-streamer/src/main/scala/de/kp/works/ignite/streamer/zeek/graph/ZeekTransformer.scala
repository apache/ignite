package de.kp.works.ignite.streamer.zeek.graph
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

import de.kp.works.ignite.file.FileEvent
import de.kp.works.ignite.mutate.IgniteMutation
import de.kp.works.ignite.streamer.zeek.BaseTransformer
import de.kp.works.ignite.transform.zeek.ZeekFormats._

object ZeekTransformer extends BaseTransformer {

  def transform(events: Seq[FileEvent]): (Seq[IgniteMutation], Seq[IgniteMutation]) = {

    try {
      /*
        * STEP #1: Collect Zeek log events with respect
        * to their eventType (which refers to the name
        * of the log file)
        */
      val data = groupEvents(events)
      /*
       * STEP #2: Persist logs for each format individually
       */
      data.map { case (format, logs) =>

        format match {
          case CAPTURE_LOSS =>
          case CONNECTION =>
          case DCE_RPC =>
          case DHCP =>
          case DNP3 =>
          case DNS =>
          case DPD =>
          case FILES =>
          case FTP =>
          case HTTP =>
          case INTEL =>
          case IRC =>
          case KERBEROS =>
          case MODBUS =>
          case MYSQL =>
          case NOTICE =>
          case NTLM =>
          case OCSP =>
          case PE =>
          case RADIUS =>
          case RDP =>
          case RFB =>
          case SIP =>
          case SMB_CMD =>
          case SMB_FILES =>
          case SMB_MAPPING =>
          case SMTP =>
          case SNMP =>
          case SOCKS =>
          case SSH =>
          case SSL =>
          case STATS =>
          case SYSLOG =>
          case TRACEROUTE =>
          case TUNNEL =>
          case WEIRD =>
          case X509 =>
          case _ => throw new Exception(s"[ZeekTransformer] Unknown format `$format.toString` detected.")

        }
      }

      ???

    } catch {
    case _: Throwable => (Seq.empty[IgniteMutation], Seq.empty[IgniteMutation])
  }

  }
}
