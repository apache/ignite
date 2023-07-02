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

import com.google.gson.{JsonElement, JsonObject}
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.json.JsonUtil
import de.kp.works.ignite.streamer.zeek.BaseUtil
import de.kp.works.ignite.transform.zeek.ZeekSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object ZeekUtil extends BaseUtil {

  private val zeekCfg = WorksConf.getCfg(WorksConf.ZEEK_CONF)
  private val zeekKey = zeekCfg.getString("primaryKey")

  private val primaryKey = StructField(zeekKey, StringType, nullable = false)

  /**
   * capture_loss (&log)
   *
   * This logs evidence regarding the degree to which the packet capture process suffers
   * from measurement loss. The loss could be due to overload on the host or NIC performing
   * the packet capture or it could even be beyond the host. If you are capturing from a
   * switch with a SPAN port, it’s very possible that the switch itself could be overloaded
   * and dropping packets.
   *
   * Reported loss is computed in terms of the number of “gap events” (ACKs for a sequence
   * number that’s above a gap).
   *
   * {
   * "ts":1568132368.465338,
   * "ts_delta":32.282249,
   * "peer":"bro",
   * "gaps":0,
   * "acks":206,
   * "percent_lost":0.0
   * }
   */
  def fromCaptureLoss(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromCaptureLoss(log.getAsJsonObject, schema)
    })
  }

  def fromCaptureLoss(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceCaptureLoss(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def capture_loss(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.capture_loss().fields
    StructType(fields)
  }

  /**
   *
   * connection (&log)
   *
   * This logs the tracking/logging of general information regarding TCP, UDP, and ICMP traffic.
   * For UDP and ICMP, “connections” are to be interpreted using flow semantics (sequence of
   * packets from a source host/port to a destination host/port). Further, ICMP “ports” are to
   * be interpreted as the source port meaning the ICMP message type and the destination port
   * being the ICMP message code.
   *
   * {
   * "ts":1547188415.857497,
   * "uid":"CAcJw21BbVedgFnYH3",
   * "id.orig_h":"192.168.86.167",
   * "id.orig_p":38339,
   * "id.resp_h":"192.168.86.1",
   * "id.resp_p":53,
   * "proto":"udp",
   * "service":"dns",
   * "duration":0.076967,
   * "orig_bytes":75,
   * "resp_bytes":178,
   * "conn_state":"SF",
   * "local_orig":true,
   * "local_resp":true,
   * "missed_bytes":0,
   * "history":"Dd",
   * "orig_pkts":1,
   * "orig_ip_bytes":103,
   * "resp_pkts":1,
   * "resp_ip_bytes":206,
   * "tunnel_parents":[]
   * }
   */

  def fromConnection(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromConnection(log.getAsJsonObject, schema)
    })
  }

  def fromConnection(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceConnection(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def connection(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.connection().fields
    StructType(fields)
  }

  /**
   * dce_rpc (&log)
   *
   * {
   * "ts":1361916332.298338,
   * "uid":"CsNHVHa1lzFtvJzT8",
   * "id.orig_h":"172.16.133.6",
   * "id.orig_p":1728,
   * "id.resp_h":"172.16.128.202",
   * "id.resp_p":445,"rtt":0.09211,
   * "named_pipe":"\u005cPIPE\u005cbrowser",
   * "endpoint":"browser",
   * "operation":"BrowserrQueryOtherDomains"
   * }
   */
  def fromDceRpc(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromDceRpc(log.getAsJsonObject, schema)
    })
  }

  def fromDceRpc(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceDceRpc(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def dce_rpc(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.dce_rpc().fields
    StructType(fields)
  }

  /**
   * dhcp (&log)
   *
   * {
   * "ts":1476605498.771847,
   * "uids":["CmWOt6VWaNGqXYcH6","CLObLo4YHn0u23Tp8a"],
   * "client_addr":"192.168.199.132",
   * "server_addr":"192.168.199.254",
   * "mac":"00:0c:29:03:df:ad",
   * "host_name":"DESKTOP-2AEFM7G",
   * "client_fqdn":"DESKTOP-2AEFM7G",
   * "domain":"localdomain",
   * "requested_addr":"192.168.199.132",
   * "assigned_addr":"192.168.199.132",
   * "lease_time":1800.0,
   * "msg_types":["REQUEST","ACK"],
   * "duration":0.000161
   * }
   */
  def fromDhcp(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromDhcp(log.getAsJsonObject, schema)
    })
  }

  def fromDhcp(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceDhcp(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def dhcp(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.dhcp().fields
    StructType(fields)
  }

  /**
   * dnp3 (&log)
   *
   * A Log of very basic DNP3 analysis script that just records
   * requests and replies.
   *
   * {
   * "ts":1227729908.705944,
   * "uid":"CQV6tj1w1t4WzQpHoe",
   * "id.orig_h":"127.0.0.1",
   * "id.orig_p":42942,
   * "id.resp_h":"127.0.0.1",
   * "id.resp_p":20000,
   * "fc_request":"READ"
   * }
   */
  def fromDnp3(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromDnp3(log.getAsJsonObject, schema)
    })
  }

  def fromDnp3(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceDnp3(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def dnp3(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.dnp3().fields
    StructType(fields)

  }

  /**
   * dns (&log)
   *
   * {
   * "ts":1547188415.857497,
   * "uid":"CAcJw21BbVedgFnYH3",
   * "id.orig_h":"192.168.86.167",
   * "id.orig_p":38339,
   * "id.resp_h":"192.168.86.1",
   * "id.resp_p":53,
   * "proto":"udp",
   * "trans_id":15209,
   * "rtt":0.076967,
   * "query":"dd625ffb4fc54735b281862aa1cd6cd4.us-west1.gcp.cloud.es.io",
   * "qclass":1,
   * "qclass_name":"C_INTERNET",
   * "qtype":1,
   * "qtype_name":"A",
   * "rcode":0,
   * "rcode_name":"NOERROR",
   * "AA":false,
   * "TC":false,
   * "RD":true,
   * "RA":true,
   * "Z":0,
   * "answers":["proxy-production-us-west1.gcp.cloud.es.io","proxy-production-us-west1-v1-009.gcp.cloud.es.io","35.199.178.4"],
   * "TTLs":[119.0,119.0,59.0],
   * "rejected":false
   * }
   */
  def fromDns(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromDns(log.getAsJsonObject, schema)
    })
  }

  def fromDns(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceDns(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def dns(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.dns().fields
    StructType(fields)
  }

  /**
   * dpd (&log)
   *
   * Dynamic protocol detection failures.
   *
   * {
   * "ts":1507567500.423033,
   * "uid":"CRrT7S1ccw9H6hzCR",
   * "id.orig_h":"192.168.10.31",
   * "id.orig_p":49285,
   * "id.resp_h":"192.168.10.10",
   * "id.resp_p":445,
   * "proto":"tcp",
   * "analyzer":"DCE_RPC",
   * "failure_reason":"Binpac exception: binpac exception: \u0026enforce violation : DCE_RPC_Header:rpc_vers"
   * }
   */
  def fromDpd(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromDpd(log.getAsJsonObject, schema)
    })
  }

  def fromDpd(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceDpd(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def dpd(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.dpd().fields
    StructType(fields)
  }

  /**
   * files (&log)
   *
   * {
   * "ts":1547688796.636812,
   * "fuid":"FMkioa222mEuM2RuQ9",
   * "tx_hosts":["35.199.178.4"],
   * "rx_hosts":["10.178.98.102"],
   * "conn_uids":["C8I0zn3r9EPbfLgta6"],
   * "source":"SSL",
   * "depth":0,
   * "analyzers":["X509","MD5","SHA1"],
   * "mime_type":"application/pkix-cert",
   * "duration":0.0,
   * "local_orig":false,
   * "is_orig":false,
   * "seen_bytes":947,
   * "missing_bytes":0,
   * "overflow_bytes":0,
   * "timedout":false,
   * "md5":"79e4a9840d7d3a96d7c04fe2434c892e",
   * "sha1":"a8985d3a65e5e5c4b2d7d66d40c6dd2fb19c5436"
   * }
   */
  def fromFiles(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromFiles(log.getAsJsonObject, schema)
    })
  }

  def fromFiles(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceFiles(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  /*
   * IMPORTANT: Despite the example above (from Filebeat),
   * the current Zeek documentation (v3.1.2) does not specify
   * a connection id
   */
  def files(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.files().fields
    StructType(fields)
  }

  /**
   * ftp (& log)
   *
   * FTP activity
   *
   * {
   * "ts":1187379104.955342,
   * "uid":"CpQoCn3o28tke89zv9",
   * "id.orig_h":"192.168.1.182",
   * "id.orig_p":62014,
   * "id.resp_h":"192.168.1.231",
   * "id.resp_p":21,
   * "user":"ftp",
   * "password":"ftp",
   * "command":"EPSV",
   * "reply_code":229,
   * "reply_msg":"Entering Extended Passive Mode (|||37100|)",
   * "data_channel.passive":true,
   * "data_channel.orig_h":"192.168.1.182",
   * "data_channel.resp_h":"192.168.1.231",
   * "data_channel.resp_p":37100
   * }
   */
  def fromFtp(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromFtp(log.getAsJsonObject, schema)
    })
  }

  def fromFtp(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceFtp(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def ftp(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.ftp().fields
    StructType(fields)
  }

  /**
   * http (&log)
   *
   * The logging model is to log request/response pairs and all
   * relevant metadata together in a single record.
   *
   * {
   * "ts":1547687130.172944,
   * "uid":"CCNp8v1SNzY7v9d1Ih",
   * "id.orig_h":"10.178.98.102",
   * "id.orig_p":62995,
   * "id.resp_h":"17.253.5.203",
   * "id.resp_p":80,
   * "trans_depth":1,
   * "method":"GET",
   * "host":"ocsp.apple.com",
   * "uri":"/ocsp04-aaica02/ME4wTKADAgEAMEUwQzBBMAkGBSsOAwIaBQAEFNqvF+Za6oA4ceFRLsAWwEInjUhJBBQx6napI3Sl39T97qDBpp7GEQ4R7AIIUP1IOZZ86ns=",
   * "version":"1.1",
   * "user_agent":"com.apple.trustd/2.0",
   * "request_body_len":0,
   * "response_body_len":3735,
   * "status_code":200,
   * "status_msg":"OK",
   * "tags":[],
   * "resp_fuids":["F5zuip1tSwASjNAHy7"],
   * "resp_mime_types":["application/ocsp-response"]
   * }
   */
  def fromHttp(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromHttp(log.getAsJsonObject, schema)
    })
  }

  def fromHttp(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceHttp(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def http(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.http().fields
    StructType(fields)
  }

  /**
   * intel (&log) - This log file is generated by Zeek's
   * intelligence framework as a more valuable information
   * compared to other (raw) log files.
   *
   * {
   * "ts":1573030980.989353,
   * "uid":"Ctefoj1tgOPt4D0EK2",
   * "id.orig_h":"192.168.1.1",
   * "id.orig_p":37598,
   * "id.resp_h":"198.41.0.4",
   * "id.resp_p":53,
   * "seen.indicator":"198.41.0.4",
   * "seen.indicator_type":"Intel::ADDR",
   * "seen.where":"Conn::IN_RESP",
   * "seen.node":"worker-1-2",
   * "matched":["Intel::ADDR"],
   * "sources":["ETPRO Rep: AbusedTLD Score: 127"]
   * }
   */
  def fromIntel(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromIntel(log.getAsJsonObject, schema)
    })
  }

  def fromIntel(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceIntel(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  /**
   * This file transforms Zeek's Intel::Info format
   * into an Apache Spark compliant Intel schema.
   *
   * Sample:
   * {
   * "ts":1320279566.452687,
   * "uid":"C4llPsinsviGyNY45",
   * "id.orig_h":"192.168.2.76",
   * "id.orig_p":52026,
   * "id.resp_h":"132.235.215.119",
   * "id.resp_p":80,
   * "seen.indicator":"www.reddit.com",
   * "seen.indicator_type":"Intel::DOMAIN",
   * "seen.where":"HTTP::IN_HOST_HEADER",
   * "seen.node":"zeek",
   * "matched":[
   * "Intel::DOMAIN"
   * ],
   * "sources":[
   * "my_special_source"
   * ]
   * }
   */
  def intel(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.intel().fields
    StructType(fields)
  }

  /**
   * irc (&log)
   *
   * The logging model is to log IRC commands along with the associated response
   * and some additional metadata about the connection if it’s available.
   *
   * {
   * "ts":1387554250.647295,
   * "uid":"CNJBX5FQdL62VUUP1",
   * "id.orig_h":"10.180.156.249",
   * "id.orig_p":45921,
   * "id.resp_h":"38.229.70.20",
   * "id.resp_p":8000,
   * "command":"USER",
   * "value":"xxxxx",
   * "addl":"+iw xxxxx XxxxxxXxxx "
   * }
   */
  def fromIrc(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromIrc(log.getAsJsonObject, schema)
    })
  }

  def fromIrc(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceIrc(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def irc(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.irc().fields
    StructType(fields)
  }

  /**
   * kerberos (&log)
   *
   * {
   * "ts":1507565599.590346,
   * "uid":"C56Flhb4WQBNkfMOl",
   * "id.orig_h":"192.168.10.31",
   * "id.orig_p":49242,
   * "id.resp_h":"192.168.10.10",
   * "id.resp_p":88,
   * "request_type":"TGS",
   * "client":"RonHD/CONTOSO.LOCAL",
   * "service":"HOST/admin-pc",
   * "success":true,
   * "till":2136422885.0,
   * "cipher":"aes256-cts-hmac-sha1-96",
   * "forwardable":true,
   * "renewable":true
   * }
   */
  def fromKerberos(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromKerberos(log.getAsJsonObject, schema)
    })
  }

  def fromKerberos(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceKerberos(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def kerberos(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.kerberos().fields
    StructType(fields)
  }

  /**
   * modbus (&log)
   *
   * {
   * "ts":1352718265.222457,
   * "uid":"CpIIXl4DFGswmjH2bl",
   * "id.orig_h":"192.168.1.10",
   * "id.orig_p":64342,
   * "id.resp_h":"192.168.1.164",
   * "id.resp_p":502,
   * "func":"READ_COILS"
   * }
   */
  def fromModbus(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromModbus(log.getAsJsonObject, schema)
    })
  }

  def fromModbus(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceModbus(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def modbus(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.modbus().fields
    StructType(fields)
  }

  /**
   * mysql (&log)
   */
  def fromMysql(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromMysql(log.getAsJsonObject, schema)
    })
  }

  def fromMysql(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceMysql(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def mysql(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.mysql().fields
    StructType(fields)
  }

  /**
   * notice (&log)
   *
   * {
   * "ts":1320435875.879278,
   * "note":"SSH::Password_Guessing",
   * "msg":"172.16.238.1 appears to be guessing SSH passwords (seen in 30 connections).",
   * "sub":"Sampled servers:  172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136",
   * "src":"172.16.238.1",
   * "peer_descr":"bro",
   * "actions":["Notice::ACTION_LOG"],
   * "suppress_for":3600.0,
   * "dropped":false
   * }
   */
  def fromNotice(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromNotice(log.getAsJsonObject, schema)
    })
  }

  def fromNotice(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceNotice(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def notice(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.notice().fields
    StructType(fields)
  }

  /**
   * ntlm (&log)
   *
   * NT LAN Manager (NTLM)
   *
   * {
   * "ts":1508959117.814467,
   * "uid":"CHphiNUKDC20fsy09",
   * "id.orig_h":"192.168.10.50",
   * "id.orig_p":46785,
   * "id.resp_h":"192.168.10.31",
   * "id.resp_p":445,
   * "username":"JeffV",
   * "hostname":"ybaARon55QykXrgu",
   * "domainname":"contoso.local",
   * "server_nb_computer_name":"VICTIM-PC",
   * "server_dns_computer_name":"Victim-PC.contoso.local",
   * "server_tree_name":"contoso.local"
   * }
   */
  def fromNtlm(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromNtlm(log.getAsJsonObject, schema)
    })
  }

  def fromNtlm(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceNtlm(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def ntlm(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.ntlm().fields
    StructType(fields)
  }

  /**
   * ocsp (&log)
   *
   * Online Certificate Status Protocol (OCSP). Only created if policy script is loaded.
   *
   */
  def fromOcsp(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromOcsp(log.getAsJsonObject, schema)
    })
  }

  def fromOcsp(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceOcsp(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def ocsp(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.ocsp().fields
    StructType(fields)
  }

  /**
   * pe (&log)
   *
   * Portable Executable (PE)
   */
  def fromPe(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromPe(log.getAsJsonObject, schema)
    })
  }

  def fromPe(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replacePe(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def pe(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.pe().fields
    StructType(fields)
  }

  /**
   * radius (&log)
   *
   * RADIUS authentication attempts
   */
  def fromRadius(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromRadius(log.getAsJsonObject, schema)
    })
  }

  def fromRadius(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceRadius(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def radius(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.radius().fields
    StructType(fields)
  }

  /**
   * rdp (&log)
   *
   * RDP Analysis
   */
  def fromRdp(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromRdp(log.getAsJsonObject, schema)
    })
  }

  def fromRdp(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceRdp(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def rdp(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.rdp().fields
    StructType(fields)
  }

  /**
   * rfb (&log)
   *
   * Remote Framebuffer (RFB)
   */
  def fromRfb(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromRfb(log.getAsJsonObject, schema)
    })
  }

  def fromRfb(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceRfb(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def rfb(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.rfb().fields
    StructType(fields)
  }

  /**
   * sip (&log)
   *
   * Log from SIP analysis. The logging model is to log request/response pairs
   * and all relevant metadata together in a single record.
   *
   * {
   * "ts":1361916159.055464,
   * "uid":"CPRLCB4eWHdjP852Bk",
   * "id.orig_h":"172.16.133.19",
   * "id.orig_p":5060,
   * "id.resp_h":"74.63.41.218",
   * "id.resp_p":5060,
   * "trans_depth":0,
   * "method":"REGISTER",
   * "uri":"sip:newyork.voip.ms:5060",
   * "request_from":"\u0022AppNeta\u0022 <sip:116954_Boston6@newyork.voip.ms>",
   * "request_to":"<sip:116954_Boston6@newyork.voip.ms>",
   * "response_from":"\u0022AppNeta\u0022 <sip:116954_Boston6@newyork.voip.ms>",
   * "response_to":"<sip:116954_Boston6@newyork.voip.ms>;tag=as023f66a5",
   * "call_id":"8694cd7e-976e4fc3-d76f6e38@172.16.133.19",
   * "seq":"4127 REGISTER",
   * "request_path":["SIP/2.0/UDP 172.16.133.19:5060"],
   * "response_path":["SIP/2.0/UDP 172.16.133.19:5060"],
   * "user_agent":"PolycomSoundStationIP-SSIP_5000-UA/3.2.4.0267",
   * "status_code":401,
   * "status_msg":"Unauthorized",
   * "request_body_len":0,
   * "response_body_len":0
   * }
   */
  def fromSip(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSip(log.getAsJsonObject, schema)
    })
  }

  def fromSip(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSip(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def sip(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.sip().fields
    StructType(fields)
  }

  /**
   * smb_cmd (&log)
   *
   * SMB commands
   *
   * {
   * "ts":1361916332.020006,
   * "uid":"CbT8mpAXseu6Pt4R7",
   * "id.orig_h":"172.16.133.6",
   * "id.orig_p":1728,
   * "id.resp_h":"172.16.128.202",
   * "id.resp_p":445,
   * "command":"NT_CREATE_ANDX",
   * "argument":"\u005cbrowser",
   * "status":"SUCCESS",
   * "rtt":0.091141,
   * "version":"SMB1",
   * "tree":"\u005c\u005cJSRVR20\u005cIPC$",
   * "tree_service":"IPC",
   * "referenced_file.ts":1361916332.020006,
   * "referenced_file.uid":"CbT8mpAXseu6Pt4R7",
   * "referenced_file.id.orig_h":"172.16.133.6",
   * "referenced_file.id.orig_p":1728,
   * "referenced_file.id.resp_h":"172.16.128.202",
   * "referenced_file.id.resp_p":445,
   * "referenced_file.action":"SMB::FILE_OPEN",
   * "referenced_file.name":"\u005cbrowser",
   * "referenced_file.size":0
   * }
   */
  def fromSmbCmd(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSmbCmd(log.getAsJsonObject, schema)
    })
  }

  def fromSmbCmd(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSmbCmd(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def smb_cmd(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.smb_cmd().fields
    StructType(fields)
  }

  /**
   * smb_files (&log)
   *
   * SMB files
   *
   * {
   * "ts":1507565599.576942,
   * "uid":"C9YAaEzWLL62yWMn5",
   * "id.orig_h":"192.168.10.31",
   * "id.orig_p":49239,
   * "id.resp_h":"192.168.10.30",
   * "id.resp_p":445,
   * "action":"SMB::FILE_OPEN",
   * "path":"\u005c\u005cadmin-pc\u005cADMIN$",
   * "name":"PSEXESVC.exe",
   * "size":0,
   * "times.modified":1507565599.607777,
   * "times.accessed":1507565599.607777,
   * "times.created":1507565599.607777,
   * "times.changed":1507565599.607777
   * }
   */
  def fromSmbFiles(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSmbFiles(log.getAsJsonObject, schema)
    })
  }

  def fromSmbFiles(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSmbFiles(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def smb_files(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.smb_files().fields
    StructType(fields)
  }

  /**
   * smb_mapping (&log)
   *
   * SMB Trees
   *
   * {
   * "ts":1507565599.576613,
   * "uid":"C9YAaEzWLL62yWMn5",
   * "id.orig_h":"192.168.10.31",
   * "id.orig_p":49239,
   * "id.resp_h":"192.168.10.30",
   * "id.resp_p":445,
   * "path":"\u005c\u005cadmin-pc\u005cADMIN$",
   * "share_type":"DISK"
   * }
   */
  def fromSmbMapping(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSmbMapping(log.getAsJsonObject, schema)
    })
  }

  def fromSmbMapping(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSmbMapping(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def smb_mapping(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.smb_mapping().fields
    StructType(fields)

  }

  /**
   * smtp (&log)
   *
   * SMTP transactions
   *
   * {
   * "ts":1543877987.381899,
   * "uid":"CWWzPB3RjqhFf528c",
   * "id.orig_h":"192.168.1.10",
   * "id.orig_p":33782,
   * "id.resp_h":"192.168.1.9",
   * "id.resp_p":25,
   * "trans_depth":1,
   * "helo":"EXAMPLE.COM",
   * "last_reply":"220 2.0.0 SMTP server ready",
   * "path":["192.168.1.9"],
   * "tls":true,
   * "fuids":[],
   * "is_webmail":false
   * }
   */
  def fromSmtp(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSmtp(log.getAsJsonObject, schema)
    })
  }

  def fromSmtp(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSmtp(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def smtp(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.smtp().fields
    StructType(fields)
  }

  /**
   * snmp (&log)
   *
   * SNMP messages
   *
   * {
   * "ts":1543877948.916584,
   * "uid":"CnKW1B4w9fpRa6Nkf2",
   * "id.orig_h":"192.168.1.2",
   * "id.orig_p":59696,
   * "id.resp_h":"192.168.1.1",
   * "id.resp_p":161,
   * "duration":7.849924,
   * "version":"2c",
   * "community":"public",
   * "get_requests":0,
   * "get_bulk_requests":0,
   * "get_responses":8,
   * "set_requests":0,
   * "up_since":1543631204.766508
   * }
   */
  def fromSnmp(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSnmp(log.getAsJsonObject, schema)
    })
  }

  def fromSnmp(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSnmp(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def snmp(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.snmp().fields
    StructType(fields)
  }

  /**
   * socks (&log)
   *
   * SOCKS proxy requests
   *
   * {
   * "ts":1566508093.09494,
   * "uid":"Cmz4Cb4qCw1hGqYw1c",
   * "id.orig_h":"127.0.0.1",
   * "id.orig_p":35368,
   * "id.resp_h":"127.0.0.1",
   * "id.resp_p":8080,
   * "version":5,
   * "status":"succeeded",
   * "request.name":"www.google.com",
   * "request_p":443,
   * "bound.host":"0.0.0.0",
   * "bound_p":0
   * }
   */
  def fromSocks(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSocks(log.getAsJsonObject, schema)
    })
  }

  def fromSocks(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSocks(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def socks(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.socks().fields
    StructType(fields)
  }

  /**
   * ssh (&log)
   *
   * {
   * "ts":1562527532.904291,
   * "uid":"CajWfz1b3qnnWT0BU9",
   * "id.orig_h":"192.168.1.2",
   * "id.orig_p":48380,
   * "id.resp_h":"192.168.1.1",
   * "id.resp_p":22,
   * "version":2,
   * "auth_success":false,
   * "auth_attempts":2,
   * "client":"SSH-2.0-OpenSSH_7.9p1 Ubuntu-10",
   * "server":"SSH-2.0-OpenSSH_6.6.1p1 Debian-4~bpo70+1",
   * "cipher_alg":"chacha20-poly1305@openssh.com",
   * "mac_alg":"umac-64-etm@openssh.com",
   * "compression_alg":"none",
   * "kex_alg":"curve25519-sha256@libssh.org",
   * "host_key_alg":"ecdsa-sha2-nistp256",
   * "host_key":"86:71:ac:9c:35:1c:28:29:05:81:48:ec:66:67:de:bd"
   * }
   */
  def fromSsh(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSsh(log.getAsJsonObject, schema)
    })
  }

  def fromSsh(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSsh(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def ssh(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.ssh().fields
    StructType(fields)
  }

  /**
   * ssl (&log)
   *
   * This logs information about the SSL/TLS handshaking
   * and encryption establishment process.
   *
   * {
   * "ts":1547688736.805088,
   * "uid":"CAOvs1BMFCX2Eh0Y3",
   * "id.orig_h":"10.178.98.102",
   * "id.orig_p":63199,
   * "id.resp_h":"35.199.178.4",
   * "id.resp_p":9243,
   * "version":"TLSv12",
   * "cipher":"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
   * "curve":"secp256r1",
   * "server_name":"dd625ffb4fc54735b281862aa1cd6cd4.us-west1.gcp.cloud.es.io",
   * "resumed":false,
   * "established":true,
   * "cert_chain_fuids":["FebkbHWVCV8rEEEne","F4BDY41MGUBT6URZMd","FWlfEfiHVkv8evDL3"],
   * "client_cert_chain_fuids":[],
   * "subject":"CN=*.gcp.cloud.es.io,O=Elasticsearch\u005c, Inc.,L=Mountain View,ST=California,C=US",
   * "issuer":"CN=DigiCert SHA2 Secure Server CA,O=DigiCert Inc,C=US",
   * "validation_status":"ok"
   * }
   */
  def fromSsl(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSsl(log.getAsJsonObject, schema)
    })
  }

  def fromSsl(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSsl(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def ssl(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.ssl().fields
    StructType(fields)
  }

  /**
   * stats (&log)
   *
   * {
   * "ts":1476605878.714844,
   * "peer":"bro",
   * "mem":94,
   * "pkts_proc":296,
   * "bytes_recv":39674,
   * "events_proc":723,
   * "events_queued":728,
   * "active_tcp_conns":1,
   * "active_udp_conns":3,
   * "active_icmp_conns":0,
   * "tcp_conns":6,
   * "udp_conns":36,
   * "icmp_conns":2,
   * "timers":797,
   * "active_timers":38,
   * "files":0,
   * "active_files":0,
   * "dns_requests":0,
   * "active_dns_requests":0,
   * "reassem_tcp_size":0,
   * "reassem_file_size":0,
   * "reassem_frag_size":0,
   * "reassem_unknown_size":0
   * }
   */
  def fromStats(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromStats(log.getAsJsonObject, schema)
    })
  }

  def fromStats(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceStats(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def stats(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.stats().fields
    StructType(fields)
  }

  /**
   * syslog (&log)
   *
   * Syslog messages.
   */
  def fromSyslog(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromSyslog(log.getAsJsonObject, schema)
    })
  }

  def fromSyslog(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceSyslog(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def syslog(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.syslog().fields
    StructType(fields)
  }

  /**
   * traceroute (&log)
   *
   * {
   * "ts":1361916158.650605,
   * "src":"192.168.1.1",
   * "dst":"8.8.8.8",
   * "proto":"udp"
   * }
   */
  def fromTraceroute(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromTraceroute(log.getAsJsonObject, schema)
    })
  }

  def fromTraceroute(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceTraceroute(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def traceroute(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.traceroute().fields
    StructType(fields)
  }

  /**
   * tunnel (&log)
   *
   * This log handles the tracking/logging of tunnels (e.g. Teredo, AYIYA, or IP-in-IP
   * such as 6to4 where “IP” is either IPv4 or IPv6).
   *
   * For any connection that occurs over a tunnel, information about its encapsulating
   * tunnels is also found in the tunnel field of connection.
   *
   * {
   * "ts":1544405666.743509,
   * "id.orig_h":"132.16.146.79",
   * "id.orig_p":0,
   * "id.resp_h":"132.16.110.133",
   * "id.resp_p":8080,
   * "tunnel_type":"Tunnel::HTTP",
   * "action":"Tunnel::DISCOVER"
   * }
   */
  def fromTunnel(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromTunnel(log.getAsJsonObject, schema)
    })
  }

  def fromTunnel(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceTunnel(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def tunnel(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.tunnel().fields
    StructType(fields)
  }

  /**
   * weird (&log)
   *
   * {
   * "ts":1543877999.99354,
   * "uid":"C1ralPp062bkwWt4e",
   * "id.orig_h":"192.168.1.1",
   * "id.orig_p":64521,
   * "id.resp_h":"192.168.1.2",
   * "id.resp_p":53,
   * "name":"dns_unmatched_reply",
   * "notice":false,
   * "peer":"worker-6"
   * }
   *
   * This log provides a default set of actions to take for “weird activity” events generated
   * from Zeek’s event engine. Weird activity is defined as unusual or exceptional activity that
   * can indicate malformed connections, traffic that doesn’t conform to a particular protocol,
   * malfunctioning or misconfigured hardware, or even an attacker attempting to avoid/confuse
   * a sensor.
   *
   * Without context, it’s hard to judge whether a particular category of weird activity is interesting,
   * but this script provides a starting point for the user.
   */
  def fromWeird(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromWeird(log.getAsJsonObject, schema)
    })
  }

  def fromWeird(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceWeird(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def weird(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.weird().fields
    StructType(fields)
  }

  /**
   * x509 (&log)
   *
   * {
   * "ts":1543867200.143484,
   * "id":"FxZ6gZ3YR6vFlIocq3",
   * "certificate.version":3,
   * "certificate.serial":"2D00003299D7071DB7D1708A42000000003299",
   * "certificate.subject":"CN=www.bing.com",
   * "certificate.issuer":"CN=Microsoft IT TLS CA 5,OU=Microsoft IT,O=Microsoft Corporation,L=Redmond,ST=Washington,C=US",
   * "certificate.not_valid_before":1500572828.0,
   * "certificate.not_valid_after":1562780828.0,
   * "certificate.key_alg":"rsaEncryption",
   * "certificate.sig_alg":"sha256WithRSAEncryption",
   * "certificate.key_type":"rsa",
   * "certificate.key_length":2048,
   * "certificate.exponent":"65537",
   * "san.dns":["www.bing.com","dict.bing.com.cn","*.platform.bing.com","*.bing.com","bing.com","ieonline.microsoft.com","*.windowssearch.com","cn.ieonline.microsoft.com","*.origin.bing.com","*.mm.bing.net","*.api.bing.com","ecn.dev.virtualearth.net","*.cn.bing.net","*.cn.bing.com","ssl-api.bing.com","ssl-api.bing.net","*.api.bing.net","*.bingapis.com","bingsandbox.com","feedback.microsoft.com","insertmedia.bing.office.net","r.bat.bing.com","*.r.bat.bing.com","*.dict.bing.com.cn","*.dict.bing.com","*.ssl.bing.com","*.appex.bing.com","*.platform.cn.bing.com","wp.m.bing.com","*.m.bing.com","global.bing.com","windowssearch.com","search.msn.com","*.bingsandbox.com","*.api.tiles.ditu.live.com","*.ditu.live.com","*.t0.tiles.ditu.live.com","*.t1.tiles.ditu.live.com","*.t2.tiles.ditu.live.com","*.t3.tiles.ditu.live.com","*.tiles.ditu.live.com","3d.live.com","api.search.live.com","beta.search.live.com","cnweb.search.live.com","dev.live.com","ditu.live.com","farecast.live.com","image.live.com","images.live.com","local.live.com.au","localsearch.live.com","ls4d.search.live.com","mail.live.com","mapindia.live.com","local.live.com","maps.live.com","maps.live.com.au","mindia.live.com","news.live.com","origin.cnweb.search.live.com","preview.local.live.com","search.live.com","test.maps.live.com","video.live.com","videos.live.com","virtualearth.live.com","wap.live.com","webmaster.live.com","webmasters.live.com","www.local.live.com.au","www.maps.live.com.au"]
   * }
   *
   */
  def fromX509(logs: Seq[JsonElement], schema: StructType): Seq[Row] = {
    logs.map(log => {
      fromX509(log.getAsJsonObject, schema)
    })
  }

  def fromX509(oldObject: JsonObject, schema: StructType): Row = {
    val newObject = replaceX509(oldObject)
    JsonUtil.json2Row(newObject, schema)
  }

  def x509(): StructType = {
    val fields = Array(primaryKey) ++ ZeekSchema.x509().fields
    StructType(fields)
  }

}
