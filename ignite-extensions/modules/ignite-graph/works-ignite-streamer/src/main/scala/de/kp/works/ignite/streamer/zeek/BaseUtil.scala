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

import com.google.gson.{JsonArray, JsonObject}
import scala.collection.JavaConversions._

trait BaseUtil {

  protected def replaceCaptureLoss(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and transform time values;
     * as these fields are declared to be non nullable, we do not check
     * their existence
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "ts_delta")

    newObject

  }

  protected def replaceConnection(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and transform time values;
     as these fields are declared to be non nullable, we do not check
     * their existence
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "duration")

    newObject = replaceConnId(newObject)
    val oldNames = List(
      "orig_bytes",
      "resp_bytes",
      "local_orig",
      "local_resp",
      "orig_pkts",
      "orig_ip_bytes",
      "resp_pkts",
      "resp_ip_bytes",
      "orig_l2_addr",
      "resp_l2_addr")

    oldNames.foreach(oldName =>
      newObject = replaceName(newObject, ZeekMapper.mapping(oldName), oldName))

    newObject

  }

  protected def replaceDceRpc(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "rtt")

    newObject = replaceConnId(newObject)
    newObject
  }

  protected def replaceDhcp(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "lease_time")

    newObject = replaceInterval(newObject, "duration")
    newObject

  }

  protected def replaceDnp3(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceDns(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "rtt")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "dns_aa", "AA")
    newObject = replaceName(newObject, "dns_tc", "TC")
    newObject = replaceName(newObject, "dns_rd", "RD")
    newObject = replaceName(newObject, "dns_ra", "RA")
    newObject = replaceName(newObject, "dns_z", "Z")

    /*
     * Rename and transform 'TTLs'
     */
    newObject = replaceName(newObject, "dns_ttls", "TTLs")
    val ttls = newObject.remove("dns_ttls").getAsJsonArray

    val new_ttls = new JsonArray()
    ttls.foreach(ttl => {

      var interval = 0L
      try {

        val ts = ttl.getAsDouble
        interval = (ts * 1000).asInstanceOf[Number].longValue

      } catch {
        case _: Throwable => /* Do nothing */
      }

      new_ttls.add(interval)

    })

    newObject.add("dns_ttls", new_ttls)
    newObject

  }

  protected def replaceDpd(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceFiles(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "duration")

    newObject = replaceName(newObject, "source_ips", "tx_hosts")
    newObject = replaceName(newObject, "destination_ips", "rx_hosts")

    newObject

  }

  protected def replaceFtp(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "data_channel_passive", "data_channel.passive")
    newObject = replaceName(newObject, "data_channel_source_ip", "data_channel.orig_h")

    newObject = replaceName(newObject, "data_channel_destination_ip", "data_channel.resp_h")
    newObject = replaceName(newObject, "data_channel_destination_port", "data_channel.resp_p")

    newObject

  }

  protected def replaceHttp(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceIntel(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "seen_indicator", "seen.indicator")
    newObject = replaceName(newObject, "seen_indicator_type", "seen.indicator_type")

    newObject = replaceName(newObject, "seen_host", "seen.host")
    newObject = replaceName(newObject, "seen_where", "seen.where")

    newObject = replaceName(newObject, "seen_node", "seen.node")

    newObject = replaceName(newObject, "cif_tags", "cif.tags")
    newObject = replaceName(newObject, "cif_confidence", "cif.confidence")

    newObject = replaceName(newObject, "cif_source", "cif.source")
    newObject = replaceName(newObject, "cif_description", "cif.description")

    newObject = replaceName(newObject, "cif_firstseen", "cif.firstseen")
    newObject = replaceName(newObject, "cif_lastseen", "cif.lastseen")

    newObject

  }

  protected def replaceIrc(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceKerberos(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceTime(newObject, "from")
    newObject = replaceTime(newObject, "till")

    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceModbus(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceMysql(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceNotice(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "suppress_for")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "source_ip", "src")
    newObject = replaceName(newObject, "destination_ip", "dst")

    newObject = replaceName(newObject, "source_port", "p")

    newObject = replaceName(newObject, "country_code", "remote_location.country_code")
    newObject = replaceName(newObject, "region", "remote_location.region")
    newObject = replaceName(newObject, "city", "remote_location.city")

    newObject = replaceName(newObject, "latitude", "remote_location.latitude")
    newObject = replaceName(newObject, "longitude", "remote_location.longitude")

    newObject

  }

  protected def replaceNtlm(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceOcsp(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceTime(newObject, "revoketime")

    newObject = replaceTime(newObject, "thisUpdate")
    newObject = replaceTime(newObject, "nextUpdate")

    newObject = replaceName(newObject, "hash_algorithm", "hashAlgorithm")
    newObject = replaceName(newObject, "issuer_name_hash", "issuerNameHash")

    newObject = replaceName(newObject, "issuer_key_hash", "issuerKeyHash")
    newObject = replaceName(newObject, "serial_number", "serialNumber")

    newObject = replaceName(newObject, "cert_status", "certStatus")
    newObject = replaceName(newObject, "revoke_time", "revoketime")


    newObject = replaceName(newObject, "revoke_reason", "revokereason")
    newObject = replaceName(newObject, "update_this", "thisUpdate")

    newObject = replaceName(newObject, "update_next", "nextUpdate")

    newObject

  }

  protected def replacePe(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceTime(newObject, "compile_ts")

    newObject

  }

  protected def replaceRadius(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "ttl")

    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceRdp(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceRfb(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceSip(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceSmbCmd(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "rtt")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "file_ts", "referenced_file.ts")
    newObject = replaceTime(newObject, "file_ts")

    newObject = replaceName(newObject, "file_uid", "referenced_file.uid")
    newObject = replaceName(newObject, "file_source_ip", "referenced_file.id.orig_h")

    newObject = replaceName(newObject, "file_source_port", "referenced_file.id.orig_p")
    newObject = replaceName(newObject, "file_destination_ip", "referenced_file.id.resp_h")

    newObject = replaceName(newObject, "file_destination_port", "referenced_file.id.resp_p")
    newObject = replaceName(newObject, "file_fuid", "referenced_file.fuid")

    newObject = replaceName(newObject, "file_action", "referenced_file.action")
    newObject = replaceName(newObject, "file_path", "referenced_file.path")

    newObject = replaceName(newObject, "file_name", "referenced_file.name")
    newObject = replaceName(newObject, "file_size", "referenced_file.size")

    newObject = replaceName(newObject, "file_prev_name", "referenced_file.prev_name")

    newObject = replaceName(newObject, "", "referenced_file.times.modified")
    newObject = replaceTime(newObject, "file_times_modified")

    newObject = replaceName(newObject, "file_times_accessed", "referenced_file.times.accessed")
    newObject = replaceTime(newObject, "file_times_accessed")

    newObject = replaceName(newObject, "file_times_created", "referenced_file.times.created")
    newObject = replaceTime(newObject, "file_times_created")

    newObject = replaceName(newObject, "file_times_changed", "referenced_file.times.changed")
    newObject = replaceTime(newObject, "file_times_changed")

    newObject

  }

  protected def replaceSmbFiles(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")

    newObject = replaceTime(newObject, "times.modified")
    newObject = replaceTime(newObject, "times.accessed")

    newObject = replaceTime(newObject, "times.created")
    newObject = replaceTime(newObject, "times.changed")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "times_modified", "times.modified")
    newObject = replaceName(newObject, "times_accessed", "times.accessed")

    newObject = replaceName(newObject, "times_created", "times.created")
    newObject = replaceName(newObject, "times_changed", "times.changed")

    newObject

  }

  protected def replaceSmbMapping(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceSmtp(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceSnmp(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceTime(newObject, "up_since")

    newObject = replaceInterval(newObject, "duration")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceSocks(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "request_host", "request.host")
    newObject = replaceName(newObject, "request_name", "request.name")
    newObject = replaceName(newObject, "request_port", "request_p")

    newObject = replaceName(newObject, "bound_host", "bound.host")
    newObject = replaceName(newObject, "bound_name", "bound.name")
    newObject = replaceName(newObject, "bound_port", "bound_p")

    newObject

  }

  protected def replaceSsh(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "country_code", "remote_location.country_code")
    newObject = replaceName(newObject, "region", "remote_location.region")
    newObject = replaceName(newObject, "city", "remote_location.city")

    newObject = replaceName(newObject, "latitude", "remote_location.latitude")
    newObject = replaceName(newObject, "longitude", "remote_location.longitude")

    newObject

  }

  protected def replaceSsl(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "notary_first_seen", "notary.first_seen")
    newObject = replaceName(newObject, "notary_last_seen", "notary.last_seen")

    newObject = replaceName(newObject, "notary_times_seen", "notary.times_seen")
    newObject = replaceName(newObject, "notary_valid", "notary.valid")

    newObject

  }

  protected def replaceStats(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "pkt_lag")

    newObject

  }

  protected def replaceSyslog(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceTraceroute(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")

    newObject = replaceName(newObject, "source_ip", "src")
    newObject = replaceName(newObject, "destination_ip", "dst")

    newObject

  }

  protected def replaceTunnel(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceWeird(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  protected def replaceX509(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceCertificate(newObject)

    newObject = replaceName(newObject, "san_dns", "san.dns")
    newObject = replaceName(newObject, "san_uri", "san.uri")

    newObject = replaceName(newObject, "san_email", "san.email")
    newObject = replaceName(newObject, "san_ip", "san.ip")

    newObject = replaceName(newObject, "san_other_fields", "san.other_fields")

    newObject = replaceName(newObject, "basic_constraints_ca", "basic_constraints.ca")
    newObject = replaceName(newObject, "basic_constraints_path_len", "basic_constraints.path_len")

    newObject

  }

  protected def replaceCertificate(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject

    newObject = replaceTime(newObject, "certificate.not_valid_before")
    newObject = replaceTime(newObject, "certificate.not_valid_after")

    newObject = replaceName(newObject, "cert_version", "certificate.version")
    newObject = replaceName(newObject, "cert_serial", "certificate.serial")

    newObject = replaceName(newObject, "cert_subject", "certificate.subject")
    newObject = replaceName(newObject, "cert_cn", "certificate.cn")

    newObject = replaceName(newObject, "cert_not_valid_before", "certificate.not_valid_before")
    newObject = replaceName(newObject, "cert_not_valid_after", "certificate.not_valid_after")

    newObject = replaceName(newObject, "cert_key_alg", "certificate.key_alg")
    newObject = replaceName(newObject, "cert_sig_alg", "certificate.sig_alg")

    newObject = replaceName(newObject, "cert_key_type", "certificate.key_type")
    newObject = replaceName(newObject, "cert_key_length", "certificate.key_length")

    newObject = replaceName(newObject, "cert_exponent", "certificate.exponent")
    newObject = replaceName(newObject, "cert_curve", "certificate.curve")

    newObject

  }

  /**
   * HINT: The provided connection parameters can be used
   * to build a unique (hash) connection identifier to join
   * with other data source like Osquery.
   */
  protected def replaceConnId(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    val oldNames = List(
      "id.orig_h",
      "id.orig_p",
      "id.resp_h",
      "id.resp_p")

    oldNames.foreach(oldName =>
      newObject = replaceName(newObject, ZeekMapper.mapping(oldName), oldName))

    newObject

  }

  /** HELPER METHOD **/

  /**
   * Zeek specifies intervals (relative time) as Double
   * that defines seconds; this method transforms them
   * into milliseconds
   */
  protected def replaceInterval(jsonObject: JsonObject, intervalName: String): JsonObject = {

    if (jsonObject == null || jsonObject.get(intervalName) == null) return jsonObject

    var interval: Long = 0L
    try {

      val ts = jsonObject.get(intervalName).getAsDouble
      interval = (ts * 1000).asInstanceOf[Number].longValue()

    } catch {
      case _: Throwable => /* Do nothing */
    }

    jsonObject.remove(intervalName)
    jsonObject.addProperty(intervalName, interval)

    jsonObject

  }

  protected def replaceName(jsonObject: JsonObject, newName: String, oldName: String): JsonObject = {

    try {

      if (jsonObject == null || jsonObject.get(oldName) == null) return jsonObject
      val value = jsonObject.remove(oldName)

      jsonObject.add(newName, value)
      jsonObject

    } catch {
      case _: Throwable => jsonObject
    }

  }

  /**
   * Zeek specifies timestamps (absolute time) as Double
   * that defines seconds; this method transforms them
   * into milliseconds
   */
  protected def replaceTime(jsonObject: JsonObject, timeName: String): JsonObject = {

    if (jsonObject == null || jsonObject.get(timeName) == null) return jsonObject

    var timestamp: Long = 0L
    try {

      val ts = jsonObject.get(timeName).getAsDouble
      timestamp = (ts * 1000).asInstanceOf[Number].longValue()

    } catch {
      case _: Throwable => /* Do nothing */
    }

    jsonObject.remove(timeName)
    jsonObject.addProperty(timeName, timestamp)

    jsonObject

  }

}
