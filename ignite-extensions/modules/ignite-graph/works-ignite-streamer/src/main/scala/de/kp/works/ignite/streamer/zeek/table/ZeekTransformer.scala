package de.kp.works.ignite.streamer.zeek.table
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
import de.kp.works.ignite.streamer.zeek.BaseTransformer
import de.kp.works.ignite.transform.zeek.ZeekFormats._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * The [ZeekTransformer] transforms 35+ Zeek log events
 * into Apache Spark schema-compliant Rows
 */
object ZeekTransformer extends BaseTransformer {

  def transform(events: Seq[FileEvent]): Seq[(ZeekFormat, StructType, Seq[Row])] = {

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
            val schema = ZeekUtil.capture_loss()
            val rows = ZeekUtil.fromCaptureLoss(logs, schema)

            (format, schema, rows)

          case CONNECTION =>
            val schema = ZeekUtil.connection()
            val rows = ZeekUtil.fromConnection(logs, schema)

            (format, schema, rows)

          case DCE_RPC =>
            val schema = ZeekUtil.dce_rpc()
            val rows = ZeekUtil.fromDceRpc(logs, schema)

            (format, schema, rows)

          case DHCP =>
            val schema = ZeekUtil.dhcp()
            val rows = ZeekUtil.fromDhcp(logs, schema)

            (format, schema, rows)

          case DNP3 =>
            val schema = ZeekUtil.dnp3()
            val rows = ZeekUtil.fromDnp3(logs, schema)

            (format, schema, rows)

          case DNS =>
            val schema = ZeekUtil.dns()
            val rows = ZeekUtil.fromDns(logs, schema)

            (format, schema, rows)

          case DPD =>
            val schema = ZeekUtil.dpd()
            val rows = ZeekUtil.fromDpd(logs, schema)

            (format, schema, rows)

          case FILES =>
            val schema = ZeekUtil.files()
            val rows = ZeekUtil.fromFiles(logs, schema)

            (format, schema, rows)

          case FTP =>
            val schema = ZeekUtil.ftp()
            val rows = ZeekUtil.fromFtp(logs, schema)

            (format, schema, rows)

          case HTTP =>
            val schema = ZeekUtil.http()
            val rows = ZeekUtil.fromHttp(logs, schema)

            (format, schema, rows)

          case INTEL =>
            val schema = ZeekUtil.intel()
            val rows = ZeekUtil.fromIntel(logs, schema)

            (format, schema, rows)

          case IRC =>
            val schema = ZeekUtil.irc()
            val rows = ZeekUtil.fromIrc(logs, schema)

            (format, schema, rows)

          case KERBEROS =>
            val schema = ZeekUtil.kerberos()
            val rows = ZeekUtil.fromKerberos(logs, schema)

            (format, schema, rows)

          case MODBUS =>
            val schema = ZeekUtil.modbus()
            val rows = ZeekUtil.fromModbus(logs, schema)

            (format, schema, rows)

          case MYSQL =>
            val schema = ZeekUtil.mysql()
            val rows = ZeekUtil.fromMysql(logs, schema)

            (format, schema, rows)

          case NOTICE =>
            val schema = ZeekUtil.notice()
            val rows = ZeekUtil.fromNotice(logs, schema)

            (format, schema, rows)

          case NTLM =>
            val schema = ZeekUtil.ntlm()
            val rows = ZeekUtil.fromNtlm(logs, schema)

            (format, schema, rows)

          case OCSP =>
            val schema = ZeekUtil.ocsp()
            val rows = ZeekUtil.fromOcsp(logs, schema)

            (format, schema, rows)

          case PE =>
            val schema = ZeekUtil.pe()
            val rows = ZeekUtil.fromPe(logs, schema)

            (format, schema, rows)

          case RADIUS =>
            val schema = ZeekUtil.radius()
            val rows = ZeekUtil.fromRadius(logs, schema)

            (format, schema, rows)

          case RDP =>
            val schema = ZeekUtil.rdp()
            val rows = ZeekUtil.fromRdp(logs, schema)

            (format, schema, rows)

          case RFB =>
            val schema = ZeekUtil.rfb()
            val rows = ZeekUtil.fromRfb(logs, schema)

            (format, schema, rows)

          case SIP =>
            val schema = ZeekUtil.sip()
            val rows = ZeekUtil.fromSip(logs, schema)

            (format, schema, rows)

          case SMB_CMD =>
            val schema = ZeekUtil.smb_cmd()
            val rows = ZeekUtil.fromSmbCmd(logs, schema)

            (format, schema, rows)

          case SMB_FILES =>
            val schema = ZeekUtil.smb_files()
            val rows = ZeekUtil.fromSmbFiles(logs, schema)

            (format, schema, rows)

          case SMB_MAPPING =>
            val schema = ZeekUtil.smb_mapping()
            val rows = ZeekUtil.fromSmbMapping(logs, schema)

            (format, schema, rows)

          case SMTP =>
            val schema = ZeekUtil.smtp()
            val rows = ZeekUtil.fromSmtp(logs, schema)

            (format, schema, rows)

          case SNMP =>
            val schema = ZeekUtil.snmp()
            val rows = ZeekUtil.fromSnmp(logs, schema)

            (format, schema, rows)

          case SOCKS =>
            val schema = ZeekUtil.socks()
            val rows = ZeekUtil.fromSocks(logs, schema)

            (format, schema, rows)

          case SSH =>
            val schema = ZeekUtil.ssh()
            val rows = ZeekUtil.fromSsh(logs, schema)

            (format, schema, rows)

          case SSL =>
            val schema = ZeekUtil.ssl()
            val rows = ZeekUtil.fromSsl(logs, schema)

            (format, schema, rows)

          case STATS =>
            val schema = ZeekUtil.stats()
            val rows = ZeekUtil.fromStats(logs, schema)

            (format, schema, rows)

          case SYSLOG =>
            val schema = ZeekUtil.syslog()
            val rows = ZeekUtil.fromSyslog(logs, schema)

            (format, schema, rows)

          case TRACEROUTE =>
            val schema = ZeekUtil.traceroute()
            val rows = ZeekUtil.fromTraceroute(logs, schema)

            (format, schema, rows)

          case TUNNEL =>
            val schema = ZeekUtil.tunnel()
            val rows = ZeekUtil.fromTunnel(logs, schema)

            (format, schema, rows)

          case WEIRD =>
            val schema = ZeekUtil.weird()
            val rows = ZeekUtil.fromWeird(logs, schema)

            (format, schema, rows)

          case X509 =>
            val schema = ZeekUtil.x509()
            val rows = ZeekUtil.fromX509(logs, schema)

            (format, schema, rows)

          case _ => throw new Exception(s"[ZeekTransformer] Unknown format `$format.toString` detected.")

        }
      }

    } catch {
      case _: Throwable => Seq.empty[(ZeekFormat, StructType, Seq[Row])]
    }

  }

}
