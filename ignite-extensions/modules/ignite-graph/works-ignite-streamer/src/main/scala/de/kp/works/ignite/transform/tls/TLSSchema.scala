package de.kp.works.ignite.transform.tls

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

/**
 * [TLSSchema] contains DataFrame compliant schema
 * specifications for all Osquery tables of v4.6.0
 */
object TLSSchema {

  def account_policy_data(): StructType = {

    val fields = Array(
      StructField("creation_time", DoubleType, nullable = true),
      StructField("failed_login_count", LongType, nullable = true),
      StructField("failed_login_timestamp", DoubleType, nullable = true),
      StructField("password_last_set_time", DoubleType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def acpi_tables(): StructType = {

    val fields = Array(
      StructField("md5", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("size", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def ad_config(): StructType = {

    val fields = Array(
      StructField("domain", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("option", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def alf(): StructType = {

    val fields = Array(
      StructField("allow_signed_enabled", IntegerType, nullable = true),
      StructField("firewall_unload", IntegerType, nullable = true),
      StructField("global_state", IntegerType, nullable = true),
      StructField("logging_enabled", IntegerType, nullable = true),
      StructField("logging_option", IntegerType, nullable = true),
      StructField("stealth_enabled", IntegerType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def alf_exceptions(): StructType = {

    val fields = Array(
      StructField("path", StringType, nullable = true),
      StructField("state", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def alf_explicit_auths(): StructType = {

    val fields = Array(
      StructField("process", StringType, nullable = true)
    )

    StructType(fields)

  }

  def app_schemes(): StructType = {

    val fields = Array(
      StructField("enabled", IntegerType, nullable = true),
      StructField("external", IntegerType, nullable = true),
      StructField("handler", StringType, nullable = true),
      StructField("protected", IntegerType, nullable = true),
      StructField("scheme", StringType, nullable = true)
    )

    StructType(fields)

  }

  def apparmor_events(): StructType = {

    val fields = Array(
      StructField("apparmor", StringType, nullable = true),
      StructField("capability", LongType, nullable = true),
      StructField("capname", StringType, nullable = true),
      StructField("comm", StringType, nullable = true),
      StructField("denied_mask", StringType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("error", StringType, nullable = true),
      StructField("fsuid", LongType, nullable = true),
      StructField("info", StringType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("message", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("namespace", StringType, nullable = true),
      StructField("operation", StringType, nullable = true),
      StructField("ouid", LongType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("profile", StringType, nullable = true),
      StructField("requested_mask", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uptime", LongType, nullable = true)
    )

    StructType(fields)

  }

  def apparmor_profiles(): StructType = {

    val fields = Array(
      StructField("attach", StringType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sha1", StringType, nullable = true)
    )

    StructType(fields)

  }

  def appcompat_shims(): StructType = {

    val fields = Array(
      StructField("description", StringType, nullable = true),
      StructField("executable", StringType, nullable = true),
      StructField("install_time", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sdb_id", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def apps(): StructType = {

    val fields = Array(
      StructField("applescript_enabled", StringType, nullable = true),
      StructField("bundle_executable", StringType, nullable = true),
      StructField("bundle_identifier", StringType, nullable = true),
      StructField("bundle_name", StringType, nullable = true),
      StructField("bundle_package_type", StringType, nullable = true),
      StructField("bundle_short_version", StringType, nullable = true),
      StructField("bundle_version", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("compiler", StringType, nullable = true),
      StructField("copyright", StringType, nullable = true),
      StructField("development_region", StringType, nullable = true),
      StructField("display_name", StringType, nullable = true),
      StructField("element", StringType, nullable = true),
      StructField("environment", StringType, nullable = true),
      StructField("info_string", StringType, nullable = true),
      StructField("last_opened_time", DoubleType, nullable = true),
      StructField("minimum_system_version", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def apt_sources(): StructType = {

    val fields = Array(
      StructField("architectures", StringType, nullable = true),
      StructField("base_uri", StringType, nullable = true),
      StructField("components", StringType, nullable = true),
      StructField("maintainer", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("release", StringType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def arp_cache(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("interface", StringType, nullable = true),
      StructField("mac", StringType, nullable = true),
      StructField("permanent", StringType, nullable = true)
    )

    StructType(fields)

  }

  def asl(): StructType = {

    val fields = Array(
      StructField("extra", StringType, nullable = true),
      StructField("facility", StringType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("host", StringType, nullable = true),
      StructField("level", IntegerType, nullable = true),
      StructField("message", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("ref_pid", IntegerType, nullable = true),
      StructField("ref_proc", StringType, nullable = true),
      StructField("sender", StringType, nullable = true),
      StructField("time", IntegerType, nullable = true),
      StructField("time_nano_sec", IntegerType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def atom_packages(): StructType = {

    val fields = Array(
      StructField("description", StringType, nullable = true),
      StructField("homepage", StringType, nullable = true),
      StructField("license", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def augeas(): StructType = {

    val fields = Array(
      StructField("label", StringType, nullable = true),
      StructField("node", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def authenticode(): StructType = {

    val fields = Array(
      StructField("issuer_name", StringType, nullable = true),
      StructField("original_program_name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("result", StringType, nullable = true),
      StructField("serial_number", StringType, nullable = true),
      StructField("subject_name", StringType, nullable = true)
    )

    StructType(fields)

  }

  def authorization_mechanisms(): StructType = {

    val fields = Array(
      StructField("entry", StringType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("mechanism", StringType, nullable = true),
      StructField("plugin", StringType, nullable = true),
      StructField("privileged", StringType, nullable = true)
    )

    StructType(fields)

  }

  def authorizations(): StructType = {

    val fields = Array(
      StructField("allow_root", StringType, nullable = true),
      StructField("authenticate_user", StringType, nullable = true),
      StructField("class", StringType, nullable = true),
      StructField("comment", StringType, nullable = true),
      StructField("created", StringType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("modified", StringType, nullable = true),
      StructField("session_owner", StringType, nullable = true),
      StructField("shared", StringType, nullable = true),
      StructField("timeout", StringType, nullable = true),
      StructField("tries", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def authorized_keys(): StructType = {

    val fields = Array(
      StructField("algorithm", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("key_file", StringType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def autoexec(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("source", StringType, nullable = true)
    )

    StructType(fields)

  }

  def azure_instance_metadata(): StructType = {

    val fields = Array(
      StructField("location", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("offer", StringType, nullable = true),
      StructField("os_type", StringType, nullable = true),
      StructField("placement_group_id", StringType, nullable = true),
      StructField("platform_fault_domain", StringType, nullable = true),
      StructField("platform_update_domain", StringType, nullable = true),
      StructField("publisher", StringType, nullable = true),
      StructField("resource_group_name", StringType, nullable = true),
      StructField("sku", StringType, nullable = true),
      StructField("subscription_id", StringType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("vm_id", StringType, nullable = true),
      StructField("vm_scale_set_name", StringType, nullable = true),
      StructField("vm_size", StringType, nullable = true),
      StructField("zone", StringType, nullable = true)
    )

    StructType(fields)

  }

  def azure_instance_tags(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true),
      StructField("vm_id", StringType, nullable = true)
    )

    StructType(fields)

  }

  def background_activities_moderator(): StructType = {

    val fields = Array(
      StructField("last_execution_time", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sid", StringType, nullable = true)
    )

    StructType(fields)

  }

  def battery(): StructType = {

    val fields = Array(
      StructField("amperage", IntegerType, nullable = true),
      StructField("charged", IntegerType, nullable = true),
      StructField("charging", IntegerType, nullable = true),
      StructField("condition", StringType, nullable = true),
      StructField("current_capacity", IntegerType, nullable = true),
      StructField("cycle_count", IntegerType, nullable = true),
      StructField("designed_capacity", IntegerType, nullable = true),
      StructField("health", StringType, nullable = true),
      StructField("manufacture_date", IntegerType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("max_capacity", IntegerType, nullable = true),
      StructField("minutes_to_full_charge", IntegerType, nullable = true),
      StructField("minutes_until_empty", IntegerType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("percent_remaining", IntegerType, nullable = true),
      StructField("serial_number", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("voltage", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def bitlocker_info(): StructType = {

    val fields = Array(
      StructField("conversion_status", IntegerType, nullable = true),
      StructField("device_id", StringType, nullable = true),
      StructField("drive_letter", StringType, nullable = true),
      StructField("encryption_method", StringType, nullable = true),
      StructField("lock_status", IntegerType, nullable = true),
      StructField("percentage_encrypted", IntegerType, nullable = true),
      StructField("persistent_volume_id", StringType, nullable = true),
      StructField("protection_status", IntegerType, nullable = true),
      StructField("version", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def block_devices(): StructType = {

    val fields = Array(
      StructField("block_size", IntegerType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("parent", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uuid", StringType, nullable = true),
      StructField("vendor", StringType, nullable = true)
    )

    StructType(fields)

  }

  def bpf_process_events(): StructType = {

    val fields = Array(
      StructField("cid", IntegerType, nullable = true),
      StructField("cmdline", StringType, nullable = true),
      StructField("cwd", StringType, nullable = true),
      StructField("duration", IntegerType, nullable = true),
      StructField("eid", IntegerType, nullable = true),
      StructField("exit_code", StringType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("json_cmdline", StringType, nullable = true),
      StructField("ntime", StringType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("probe_error", IntegerType, nullable = true),
      StructField("syscall", StringType, nullable = true),
      StructField("tid", LongType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def bpf_socket_events(): StructType = {

    val fields = Array(
      StructField("cid", IntegerType, nullable = true),
      StructField("duration", IntegerType, nullable = true),
      StructField("eid", IntegerType, nullable = true),
      StructField("exit_code", StringType, nullable = true),
      StructField("family", IntegerType, nullable = true),
      StructField("fd", StringType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("local_address", StringType, nullable = true),
      StructField("local_port", IntegerType, nullable = true),
      StructField("ntime", StringType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("probe_error", IntegerType, nullable = true),
      StructField("protocol", IntegerType, nullable = true),
      StructField("remote_address", StringType, nullable = true),
      StructField("remote_port", IntegerType, nullable = true),
      StructField("syscall", StringType, nullable = true),
      StructField("tid", LongType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("type", IntegerType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def browser_plugins(): StructType = {

    val fields = Array(
      StructField("description", StringType, nullable = true),
      StructField("development_region", StringType, nullable = true),
      StructField("disabled", IntegerType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("native", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sdk", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def carbon_black_info(): StructType = {

    val fields = Array(
      StructField("binary_queue", IntegerType, nullable = true),
      StructField("collect_cross_processes", IntegerType, nullable = true),
      StructField("collect_data_file_writes", IntegerType, nullable = true),
      StructField("collect_emet_events", IntegerType, nullable = true),
      StructField("collect_file_mods", IntegerType, nullable = true),
      StructField("collect_module_info", IntegerType, nullable = true),
      StructField("collect_module_loads", IntegerType, nullable = true),
      StructField("collect_net_conns", IntegerType, nullable = true),
      StructField("collect_process_user_context", IntegerType, nullable = true),
      StructField("collect_processes", IntegerType, nullable = true),
      StructField("collect_reg_mods", IntegerType, nullable = true),
      StructField("collect_sensor_operations", IntegerType, nullable = true),
      StructField("collect_store_files", IntegerType, nullable = true),
      StructField("config_name", StringType, nullable = true),
      StructField("event_queue", IntegerType, nullable = true),
      StructField("log_file_disk_quota_mb", IntegerType, nullable = true),
      StructField("log_file_disk_quota_percentage", IntegerType, nullable = true),
      StructField("protection_disabled", IntegerType, nullable = true),
      StructField("sensor_backend_server", StringType, nullable = true),
      StructField("sensor_id", IntegerType, nullable = true),
      StructField("sensor_ip_addr", StringType, nullable = true)
    )

    StructType(fields)

  }

  def carves(): StructType = {

    val fields = Array(
      StructField("carve", IntegerType, nullable = true),
      StructField("carve_guid", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("request_id", StringType, nullable = true),
      StructField("sha256", StringType, nullable = true),
      StructField("size", IntegerType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("time", LongType, nullable = true)
    )

    StructType(fields)

  }

  def certificates(): StructType = {

    val fields = Array(
      StructField("authority_key_id", StringType, nullable = true),
      StructField("ca", IntegerType, nullable = true),
      StructField("common_name", StringType, nullable = true),
      StructField("issuer", StringType, nullable = true),
      StructField("key_algorithm", StringType, nullable = true),
      StructField("key_strength", StringType, nullable = true),
      StructField("key_usage", StringType, nullable = true),
      StructField("not_valid_after", StringType, nullable = true),
      StructField("not_valid_before", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("self_signed", IntegerType, nullable = true),
      StructField("serial", StringType, nullable = true),
      StructField("sha1", StringType, nullable = true),
      StructField("sid", StringType, nullable = true),
      StructField("signing_algorithm", StringType, nullable = true),
      StructField("store", StringType, nullable = true),
      StructField("store_id", StringType, nullable = true),
      StructField("store_location", StringType, nullable = true),
      StructField("subject", StringType, nullable = true),
      StructField("subject_key_id", StringType, nullable = true),
      StructField("username", StringType, nullable = true)
    )

    StructType(fields)

  }

  def chassis_info(): StructType = {

    val fields = Array(
      StructField("audible_alarm", StringType, nullable = true),
      StructField("breach_description", StringType, nullable = true),
      StructField("chassis_types", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("lock", StringType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("security_breach", StringType, nullable = true),
      StructField("serial", StringType, nullable = true),
      StructField("sku", StringType, nullable = true),
      StructField("smbios_tag", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("visible_alarm", StringType, nullable = true)
    )

    StructType(fields)

  }

  def chocolatey_packages(): StructType = {

    val fields = Array(
      StructField("author", StringType, nullable = true),
      StructField("license", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("summary", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def chrome_extension_content_scripts(): StructType = {

    val fields = Array(
      StructField("identifier", StringType, nullable = true),
      StructField("match", StringType, nullable = true),
      StructField("script", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def chrome_extensions(): StructType = {

    val fields = Array(
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("locale", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("optional_permissions", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("permissions", StringType, nullable = true),
      StructField("persistent", IntegerType, nullable = true),
      StructField("profile", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("update_url", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def connectivity(): StructType = {

    val fields = Array(
      StructField("disconnected", IntegerType, nullable = true),
      StructField("ipv4_internet", IntegerType, nullable = true),
      StructField("ipv4_local_network", IntegerType, nullable = true),
      StructField("ipv4_no_traffic", IntegerType, nullable = true),
      StructField("ipv4_subnet", IntegerType, nullable = true),
      StructField("ipv6_internet", IntegerType, nullable = true),
      StructField("ipv6_local_network", IntegerType, nullable = true),
      StructField("ipv6_no_traffic", IntegerType, nullable = true),
      StructField("ipv6_subnet", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def cpu_info(): StructType = {

    val fields = Array(
      StructField("address_width", StringType, nullable = true),
      StructField("availability", StringType, nullable = true),
      StructField("cpu_status", IntegerType, nullable = true),
      StructField("current_clock_speed", IntegerType, nullable = true),
      StructField("device_id", StringType, nullable = true),
      StructField("logical_processors", IntegerType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("max_clock_speed", IntegerType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("number_of_cores", StringType, nullable = true),
      StructField("processor_type", StringType, nullable = true),
      StructField("socket_designation", StringType, nullable = true)
    )

    StructType(fields)

  }

  def cpu_time(): StructType = {

    val fields = Array(
      StructField("core", IntegerType, nullable = true),
      StructField("guest", LongType, nullable = true),
      StructField("guest_nice", LongType, nullable = true),
      StructField("idle", LongType, nullable = true),
      StructField("iowait", LongType, nullable = true),
      StructField("irq", LongType, nullable = true),
      StructField("nice", LongType, nullable = true),
      StructField("softirq", LongType, nullable = true),
      StructField("steal", LongType, nullable = true),
      StructField("system", LongType, nullable = true),
      StructField("user", LongType, nullable = true)
    )

    StructType(fields)

  }

  def cpuid(): StructType = {

    val fields = Array(
      StructField("feature", StringType, nullable = true),
      StructField("input_eax", StringType, nullable = true),
      StructField("output_bit", IntegerType, nullable = true),
      StructField("output_register", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def crashes(): StructType = {

    val fields = Array(
      StructField("crash_path", StringType, nullable = true),
      StructField("crashed_thread", LongType, nullable = true),
      StructField("datetime", StringType, nullable = true),
      StructField("exception_codes", StringType, nullable = true),
      StructField("exception_notes", StringType, nullable = true),
      StructField("exception_type", StringType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("registers", StringType, nullable = true),
      StructField("responsible", StringType, nullable = true),
      StructField("stack_trace", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uid", IntegerType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def crontab(): StructType = {

    val fields = Array(
      StructField("command", StringType, nullable = true),
      StructField("day_of_month", StringType, nullable = true),
      StructField("day_of_week", StringType, nullable = true),
      StructField("event", StringType, nullable = true),
      StructField("hour", StringType, nullable = true),
      StructField("minute", StringType, nullable = true),
      StructField("month", StringType, nullable = true),
      StructField("path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def cups_destinations(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("option_name", StringType, nullable = true),
      StructField("option_value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def cups_jobs(): StructType = {

    val fields = Array(
      StructField("completed_time", IntegerType, nullable = true),
      StructField("creation_time", IntegerType, nullable = true),
      StructField("destination", StringType, nullable = true),
      StructField("format", StringType, nullable = true),
      StructField("processing_time", IntegerType, nullable = true),
      StructField("size", IntegerType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("user", StringType, nullable = true)
    )

    StructType(fields)

  }

  def curl(): StructType = {

    val fields = Array(
      StructField("bytes", LongType, nullable = true),
      StructField("method", StringType, nullable = true),
      StructField("response_code", IntegerType, nullable = true),
      StructField("result", StringType, nullable = true),
      StructField("round_trip_time", LongType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("user_agent", StringType, nullable = true)
    )

    StructType(fields)

  }

  def curl_certificate(): StructType = {

    val fields = Array(
      StructField("authority_key_identifier", StringType, nullable = true),
      StructField("basic_constraint", StringType, nullable = true),
      StructField("common_name", StringType, nullable = true),
      StructField("dump_certificate", IntegerType, nullable = true),
      StructField("extended_key_usage", StringType, nullable = true),
      StructField("has_expired", IntegerType, nullable = true),
      StructField("hostname", StringType, nullable = true),
      StructField("info_access", StringType, nullable = true),
      StructField("issuer_alternative_names", StringType, nullable = true),
      StructField("issuer_common_name", StringType, nullable = true),
      StructField("issuer_organization", StringType, nullable = true),
      StructField("issuer_organization_unit", StringType, nullable = true),
      StructField("key_usage", StringType, nullable = true),
      StructField("name_constraints", StringType, nullable = true),
      StructField("organization", StringType, nullable = true),
      StructField("organization_unit", StringType, nullable = true),
      StructField("pem", StringType, nullable = true),
      StructField("policies", StringType, nullable = true),
      StructField("policy_constraints", StringType, nullable = true),
      StructField("policy_mappings", StringType, nullable = true),
      StructField("serial_number", StringType, nullable = true),
      StructField("sha1_fingerprint", StringType, nullable = true),
      StructField("sha256_fingerprint", StringType, nullable = true),
      StructField("signature", StringType, nullable = true),
      StructField("signature_algorithm", StringType, nullable = true),
      StructField("subject_alternative_names", StringType, nullable = true),
      StructField("subject_info_access", StringType, nullable = true),
      StructField("subject_key_identifier", StringType, nullable = true),
      StructField("timeout", IntegerType, nullable = true),
      StructField("valid_from", StringType, nullable = true),
      StructField("valid_to", StringType, nullable = true),
      StructField("version", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def deb_packages(): StructType = {

    val fields = Array(
      StructField("arch", StringType, nullable = true),
      StructField("maintainer", StringType, nullable = true),
      StructField("mount_namespace_id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("pid_with_namespace", IntegerType, nullable = true),
      StructField("priority", StringType, nullable = true),
      StructField("revision", StringType, nullable = true),
      StructField("section", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def default_environment(): StructType = {

    val fields = Array(
      StructField("expand", IntegerType, nullable = true),
      StructField("value", StringType, nullable = true),
      StructField("variable", StringType, nullable = true)
    )

    StructType(fields)

  }

  def device_file(): StructType = {

    val fields = Array(
      StructField("atime", LongType, nullable = true),
      StructField("block_size", IntegerType, nullable = true),
      StructField("ctime", LongType, nullable = true),
      StructField("device", StringType, nullable = true),
      StructField("filename", StringType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("hard_links", IntegerType, nullable = true),
      StructField("inode", LongType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("mtime", LongType, nullable = true),
      StructField("partition", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def device_firmware(): StructType = {

    val fields = Array(
      StructField("device", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def device_hash(): StructType = {

    val fields = Array(
      StructField("device", StringType, nullable = true),
      StructField("inode", LongType, nullable = true),
      StructField("md5", StringType, nullable = true),
      StructField("partition", StringType, nullable = true),
      StructField("sha1", StringType, nullable = true),
      StructField("sha256", StringType, nullable = true)
    )

    StructType(fields)

  }

  def device_partitions(): StructType = {

    val fields = Array(
      StructField("blocks", LongType, nullable = true),
      StructField("blocks_size", LongType, nullable = true),
      StructField("device", StringType, nullable = true),
      StructField("flags", IntegerType, nullable = true),
      StructField("inodes", LongType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("offset", LongType, nullable = true),
      StructField("partition", IntegerType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def disk_encryption(): StructType = {

    val fields = Array(
      StructField("encrypted", IntegerType, nullable = true),
      StructField("encryption_status", StringType, nullable = true),
      StructField("filevault_status", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uid", StringType, nullable = true),
      StructField("user_uuid", StringType, nullable = true),
      StructField("uuid", StringType, nullable = true)
    )

    StructType(fields)

  }

  def disk_events(): StructType = {

    val fields = Array(
      StructField("action", StringType, nullable = true),
      StructField("checksum", StringType, nullable = true),
      StructField("content", StringType, nullable = true),
      StructField("device", StringType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("ejectable", IntegerType, nullable = true),
      StructField("filesystem", StringType, nullable = true),
      StructField("media_name", StringType, nullable = true),
      StructField("mountable", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("uuid", StringType, nullable = true),
      StructField("vendor", StringType, nullable = true),
      StructField("writable", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def disk_info(): StructType = {

    val fields = Array(
      StructField("description", StringType, nullable = true),
      StructField("disk_index", IntegerType, nullable = true),
      StructField("disk_size", LongType, nullable = true),
      StructField("hardware_model", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("partitions", IntegerType, nullable = true),
      StructField("pnp_device_id", StringType, nullable = true),
      StructField("serial", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def dns_cache(): StructType = {

    val fields = Array(
      StructField("flags", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def dns_resolvers(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("netmask", StringType, nullable = true),
      StructField("options", LongType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_container_fs_changes(): StructType = {

    val fields = Array(
      StructField("change_type", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_container_labels(): StructType = {

    val fields = Array(
      StructField("id", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_container_mounts(): StructType = {

    val fields = Array(
      StructField("destination", StringType, nullable = true),
      StructField("driver", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("propagation", StringType, nullable = true),
      StructField("rw", IntegerType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_container_networks(): StructType = {

    val fields = Array(
      StructField("endpoint_id", StringType, nullable = true),
      StructField("gateway", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("ip_address", StringType, nullable = true),
      StructField("ip_prefix_len", IntegerType, nullable = true),
      StructField("ipv6_address", StringType, nullable = true),
      StructField("ipv6_gateway", StringType, nullable = true),
      StructField("ipv6_prefix_len", IntegerType, nullable = true),
      StructField("mac_address", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("network_id", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_container_ports(): StructType = {

    val fields = Array(
      StructField("host_ip", StringType, nullable = true),
      StructField("host_port", IntegerType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("port", IntegerType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_container_processes(): StructType = {

    val fields = Array(
      StructField("cmdline", StringType, nullable = true),
      StructField("cpu", DoubleType, nullable = true),
      StructField("egid", LongType, nullable = true),
      StructField("euid", LongType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("mem", DoubleType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("nice", IntegerType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("pgroup", LongType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("resident_size", LongType, nullable = true),
      StructField("sgid", LongType, nullable = true),
      StructField("start_time", LongType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("suid", LongType, nullable = true),
      StructField("threads", IntegerType, nullable = true),
      StructField("time", StringType, nullable = true),
      StructField("total_size", LongType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("user", StringType, nullable = true),
      StructField("wired_size", LongType, nullable = true)
    )

    StructType(fields)

  }

  def docker_container_stats(): StructType = {

    val fields = Array(
      StructField("cpu_kernelmode_usage", LongType, nullable = true),
      StructField("cpu_total_usage", LongType, nullable = true),
      StructField("cpu_usermode_usage", LongType, nullable = true),
      StructField("disk_read", LongType, nullable = true),
      StructField("disk_write", LongType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("interval", LongType, nullable = true),
      StructField("memory_limit", LongType, nullable = true),
      StructField("memory_max_usage", LongType, nullable = true),
      StructField("memory_usage", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("network_rx_bytes", LongType, nullable = true),
      StructField("network_tx_bytes", LongType, nullable = true),
      StructField("num_procs", IntegerType, nullable = true),
      StructField("online_cpus", IntegerType, nullable = true),
      StructField("pids", IntegerType, nullable = true),
      StructField("pre_cpu_kernelmode_usage", LongType, nullable = true),
      StructField("pre_cpu_total_usage", LongType, nullable = true),
      StructField("pre_cpu_usermode_usage", LongType, nullable = true),
      StructField("pre_online_cpus", IntegerType, nullable = true),
      StructField("pre_system_cpu_usage", LongType, nullable = true),
      StructField("preread", LongType, nullable = true),
      StructField("read", LongType, nullable = true),
      StructField("system_cpu_usage", LongType, nullable = true)
    )

    StructType(fields)

  }

  def docker_containers(): StructType = {

    val fields = Array(
      StructField("cgroup_namespace", StringType, nullable = true),
      StructField("command", StringType, nullable = true),
      StructField("config_entrypoint", StringType, nullable = true),
      StructField("created", LongType, nullable = true),
      StructField("env_variables", StringType, nullable = true),
      StructField("finished_at", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("image", StringType, nullable = true),
      StructField("image_id", StringType, nullable = true),
      StructField("ipc_namespace", StringType, nullable = true),
      StructField("mnt_namespace", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("net_namespace", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("pid_namespace", StringType, nullable = true),
      StructField("privileged", IntegerType, nullable = true),
      StructField("readonly_rootfs", IntegerType, nullable = true),
      StructField("security_options", StringType, nullable = true),
      StructField("started_at", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("user_namespace", StringType, nullable = true),
      StructField("uts_namespace", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_image_history(): StructType = {

    val fields = Array(
      StructField("comment", StringType, nullable = true),
      StructField("created", LongType, nullable = true),
      StructField("created_by", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("tags", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_image_labels(): StructType = {

    val fields = Array(
      StructField("id", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_image_layers(): StructType = {

    val fields = Array(
      StructField("id", StringType, nullable = true),
      StructField("layer_id", StringType, nullable = true),
      StructField("layer_order", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def docker_images(): StructType = {

    val fields = Array(
      StructField("created", LongType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("size_bytes", LongType, nullable = true),
      StructField("tags", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_info(): StructType = {

    val fields = Array(
      StructField("architecture", StringType, nullable = true),
      StructField("bridge_nf_ip6tables", IntegerType, nullable = true),
      StructField("bridge_nf_iptables", IntegerType, nullable = true),
      StructField("cgroup_driver", StringType, nullable = true),
      StructField("containers", IntegerType, nullable = true),
      StructField("containers_paused", IntegerType, nullable = true),
      StructField("containers_running", IntegerType, nullable = true),
      StructField("containers_stopped", IntegerType, nullable = true),
      StructField("cpu_cfs_period", IntegerType, nullable = true),
      StructField("cpu_cfs_quota", IntegerType, nullable = true),
      StructField("cpu_set", IntegerType, nullable = true),
      StructField("cpu_shares", IntegerType, nullable = true),
      StructField("cpus", IntegerType, nullable = true),
      StructField("http_proxy", StringType, nullable = true),
      StructField("https_proxy", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("images", IntegerType, nullable = true),
      StructField("ipv4_forwarding", IntegerType, nullable = true),
      StructField("kernel_memory", IntegerType, nullable = true),
      StructField("kernel_version", StringType, nullable = true),
      StructField("logging_driver", StringType, nullable = true),
      StructField("memory", LongType, nullable = true),
      StructField("memory_limit", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("no_proxy", StringType, nullable = true),
      StructField("oom_kill_disable", IntegerType, nullable = true),
      StructField("os", StringType, nullable = true),
      StructField("os_type", StringType, nullable = true),
      StructField("root_dir", StringType, nullable = true),
      StructField("server_version", StringType, nullable = true),
      StructField("storage_driver", StringType, nullable = true),
      StructField("swap_limit", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def docker_network_labels(): StructType = {

    val fields = Array(
      StructField("id", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_networks(): StructType = {

    val fields = Array(
      StructField("created", LongType, nullable = true),
      StructField("driver", StringType, nullable = true),
      StructField("enable_ipv6", IntegerType, nullable = true),
      StructField("gateway", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("subnet", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_version(): StructType = {

    val fields = Array(
      StructField("api_version", StringType, nullable = true),
      StructField("arch", StringType, nullable = true),
      StructField("build_time", StringType, nullable = true),
      StructField("git_commit", StringType, nullable = true),
      StructField("go_version", StringType, nullable = true),
      StructField("kernel_version", StringType, nullable = true),
      StructField("min_api_version", StringType, nullable = true),
      StructField("os", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_volume_labels(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def docker_volumes(): StructType = {

    val fields = Array(
      StructField("driver", StringType, nullable = true),
      StructField("mount_point", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def drivers(): StructType = {

    val fields = Array(
      StructField("class", StringType, nullable = true),
      StructField("date", LongType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("device_id", StringType, nullable = true),
      StructField("device_name", StringType, nullable = true),
      StructField("driver_key", StringType, nullable = true),
      StructField("image", StringType, nullable = true),
      StructField("inf", StringType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("provider", StringType, nullable = true),
      StructField("service", StringType, nullable = true),
      StructField("service_key", StringType, nullable = true),
      StructField("signed", IntegerType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def ec2_instance_metadata(): StructType = {

    val fields = Array(
      StructField("account_id", StringType, nullable = true),
      StructField("ami_id", StringType, nullable = true),
      StructField("architecture", StringType, nullable = true),
      StructField("availability_zone", StringType, nullable = true),
      StructField("iam_arn", StringType, nullable = true),
      StructField("instance_id", StringType, nullable = true),
      StructField("instance_type", StringType, nullable = true),
      StructField("local_hostname", StringType, nullable = true),
      StructField("local_ipv4", StringType, nullable = true),
      StructField("mac", StringType, nullable = true),
      StructField("region", StringType, nullable = true),
      StructField("reservation_id", StringType, nullable = true),
      StructField("security_groups", StringType, nullable = true),
      StructField("ssh_public_key", StringType, nullable = true)
    )

    StructType(fields)

  }

  def ec2_instance_tags(): StructType = {

    val fields = Array(
      StructField("instance_id", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def elf_dynamic(): StructType = {

    val fields = Array(
      StructField("class", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("tag", IntegerType, nullable = true),
      StructField("value", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def elf_info(): StructType = {

    val fields = Array(
      StructField("abi", StringType, nullable = true),
      StructField("abi_version", IntegerType, nullable = true),
      StructField("class", StringType, nullable = true),
      StructField("entry", LongType, nullable = true),
      StructField("flags", IntegerType, nullable = true),
      StructField("machine", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("version", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def elf_sections(): StructType = {

    val fields = Array(
      StructField("align", IntegerType, nullable = true),
      StructField("flags", StringType, nullable = true),
      StructField("link", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("offset", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("size", IntegerType, nullable = true),
      StructField("type", IntegerType, nullable = true),
      StructField("vaddr", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def elf_segments(): StructType = {

    val fields = Array(
      StructField("align", IntegerType, nullable = true),
      StructField("flags", StringType, nullable = true),
      StructField("msize", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("offset", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("psize", IntegerType, nullable = true),
      StructField("vaddr", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def elf_symbols(): StructType = {

    val fields = Array(
      StructField("addr", IntegerType, nullable = true),
      StructField("binding", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("offset", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("size", IntegerType, nullable = true),
      StructField("table", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def etc_hosts(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("hostnames", StringType, nullable = true)
    )

    StructType(fields)

  }

  def etc_protocols(): StructType = {

    val fields = Array(
      StructField("alias", StringType, nullable = true),
      StructField("comment", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def etc_services(): StructType = {

    val fields = Array(
      StructField("aliases", StringType, nullable = true),
      StructField("comment", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("port", IntegerType, nullable = true),
      StructField("protocol", StringType, nullable = true)
    )

    StructType(fields)

  }

  def event_taps(): StructType = {

    val fields = Array(
      StructField("enabled", IntegerType, nullable = true),
      StructField("event_tap_id", IntegerType, nullable = true),
      StructField("event_tapped", StringType, nullable = true),
      StructField("process_being_tapped", IntegerType, nullable = true),
      StructField("tapping_process", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def example(): StructType = {

    val fields = Array(
      StructField("action", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("points", IntegerType, nullable = true),
      StructField("size", LongType, nullable = true)
    )

    StructType(fields)

  }

  def extended_attributes(): StructType = {

    val fields = Array(
      StructField("base64", IntegerType, nullable = true),
      StructField("directory", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def fan_speed_sensors(): StructType = {

    val fields = Array(
      StructField("actual", IntegerType, nullable = true),
      StructField("fan", StringType, nullable = true),
      StructField("max", IntegerType, nullable = true),
      StructField("min", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("target", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def fbsd_kmods(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("refs", IntegerType, nullable = true),
      StructField("size", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def file(): StructType = {

    val fields = Array(
      StructField("atime", LongType, nullable = true),
      StructField("attributes", StringType, nullable = true),
      StructField("block_size", IntegerType, nullable = true),
      StructField("bsd_flags", StringType, nullable = true),
      StructField("btime", LongType, nullable = true),
      StructField("ctime", LongType, nullable = true),
      StructField("device", LongType, nullable = true),
      StructField("directory", StringType, nullable = true),
      StructField("file_id", StringType, nullable = true),
      StructField("file_version", StringType, nullable = true),
      StructField("filename", StringType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("hard_links", IntegerType, nullable = true),
      StructField("inode", LongType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("mount_namespace_id", StringType, nullable = true),
      StructField("mtime", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid_with_namespace", IntegerType, nullable = true),
      StructField("product_version", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("symlink", IntegerType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("volume_serial", StringType, nullable = true)
    )

    StructType(fields)

  }

  def file_events(): StructType = {

    val fields = Array(
      StructField("action", StringType, nullable = true),
      StructField("atime", LongType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("ctime", LongType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("hashed", IntegerType, nullable = true),
      StructField("inode", LongType, nullable = true),
      StructField("md5", StringType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("mtime", LongType, nullable = true),
      StructField("sha1", StringType, nullable = true),
      StructField("sha256", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("target_path", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("transaction_id", LongType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def firefox_addons(): StructType = {

    val fields = Array(
      StructField("active", IntegerType, nullable = true),
      StructField("autoupdate", IntegerType, nullable = true),
      StructField("creator", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("disabled", IntegerType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("native", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("source_url", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("visible", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def gatekeeper(): StructType = {

    val fields = Array(
      StructField("assessments_enabled", IntegerType, nullable = true),
      StructField("dev_id_enabled", IntegerType, nullable = true),
      StructField("opaque_version", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def gatekeeper_approved_apps(): StructType = {

    val fields = Array(
      StructField("ctime", DoubleType, nullable = true),
      StructField("mtime", DoubleType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("requirement", StringType, nullable = true)
    )

    StructType(fields)

  }

  def groups(): StructType = {

    val fields = Array(
      StructField("comment", StringType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("gid_signed", LongType, nullable = true),
      StructField("group_sid", StringType, nullable = true),
      StructField("groupname", StringType, nullable = true),
      StructField("is_hidden", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def hardware_events(): StructType = {

    val fields = Array(
      StructField("action", StringType, nullable = true),
      StructField("driver", StringType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("model_id", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("revision", StringType, nullable = true),
      StructField("serial", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("vendor", StringType, nullable = true),
      StructField("vendor_id", StringType, nullable = true)
    )

    StructType(fields)

  }

  def hash(): StructType = {

    val fields = Array(
      StructField("directory", StringType, nullable = true),
      StructField("md5", StringType, nullable = true),
      StructField("mount_namespace_id", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid_with_namespace", IntegerType, nullable = true),
      StructField("sha1", StringType, nullable = true),
      StructField("sha256", StringType, nullable = true),
      StructField("ssdeep", StringType, nullable = true)
    )

    StructType(fields)

  }

  def homebrew_packages(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def hvci_status(): StructType = {

    val fields = Array(
      StructField("code_integrity_policy_enforcement_status", StringType, nullable = true),
      StructField("instance_identifier", StringType, nullable = true),
      StructField("umci_policy_status", StringType, nullable = true),
      StructField("vbs_status", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def ibridge_info(): StructType = {

    val fields = Array(
      StructField("boot_uuid", StringType, nullable = true),
      StructField("coprocessor_version", StringType, nullable = true),
      StructField("firmware_version", StringType, nullable = true),
      StructField("unique_chip_id", StringType, nullable = true)
    )

    StructType(fields)

  }

  def ie_extensions(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("registry_path", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def intel_me_info(): StructType = {

    val fields = Array(
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def interface_addresses(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("broadcast", StringType, nullable = true),
      StructField("friendly_name", StringType, nullable = true),
      StructField("interface", StringType, nullable = true),
      StructField("mask", StringType, nullable = true),
      StructField("point_to_point", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def interface_details(): StructType = {

    val fields = Array(
      StructField("collisions", LongType, nullable = true),
      StructField("connection_id", StringType, nullable = true),
      StructField("connection_status", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("dhcp_enabled", IntegerType, nullable = true),
      StructField("dhcp_lease_expires", StringType, nullable = true),
      StructField("dhcp_lease_obtained", StringType, nullable = true),
      StructField("dhcp_server", StringType, nullable = true),
      StructField("dns_domain", StringType, nullable = true),
      StructField("dns_domain_suffix_search_order", StringType, nullable = true),
      StructField("dns_host_name", StringType, nullable = true),
      StructField("dns_server_search_order", StringType, nullable = true),
      StructField("enabled", IntegerType, nullable = true),
      StructField("flags", IntegerType, nullable = true),
      StructField("friendly_name", StringType, nullable = true),
      StructField("ibytes", LongType, nullable = true),
      StructField("idrops", LongType, nullable = true),
      StructField("ierrors", LongType, nullable = true),
      StructField("interface", StringType, nullable = true),
      StructField("ipackets", LongType, nullable = true),
      StructField("last_change", LongType, nullable = true),
      StructField("link_speed", LongType, nullable = true),
      StructField("mac", StringType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("metric", IntegerType, nullable = true),
      StructField("mtu", IntegerType, nullable = true),
      StructField("obytes", LongType, nullable = true),
      StructField("odrops", LongType, nullable = true),
      StructField("oerrors", LongType, nullable = true),
      StructField("opackets", LongType, nullable = true),
      StructField("pci_slot", StringType, nullable = true),
      StructField("physical_adapter", IntegerType, nullable = true),
      StructField("service", StringType, nullable = true),
      StructField("speed", IntegerType, nullable = true),
      StructField("type", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def interface_ipv6(): StructType = {

    val fields = Array(
      StructField("forwarding_enabled", IntegerType, nullable = true),
      StructField("hop_limit", IntegerType, nullable = true),
      StructField("interface", StringType, nullable = true),
      StructField("redirect_accept", IntegerType, nullable = true),
      StructField("rtadv_accept", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def iokit_devicetree(): StructType = {

    val fields = Array(
      StructField("busy_state", IntegerType, nullable = true),
      StructField("class", StringType, nullable = true),
      StructField("depth", IntegerType, nullable = true),
      StructField("device_path", StringType, nullable = true),
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("retain_count", IntegerType, nullable = true),
      StructField("service", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def iokit_registry(): StructType = {

    val fields = Array(
      StructField("busy_state", IntegerType, nullable = true),
      StructField("class", StringType, nullable = true),
      StructField("depth", IntegerType, nullable = true),
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("retain_count", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def iptables(): StructType = {

    val fields = Array(
      StructField("bytes", IntegerType, nullable = true),
      StructField("chain", StringType, nullable = true),
      StructField("dst_ip", StringType, nullable = true),
      StructField("dst_mask", StringType, nullable = true),
      StructField("dst_port", StringType, nullable = true),
      StructField("filter_name", StringType, nullable = true),
      StructField("iniface", StringType, nullable = true),
      StructField("iniface_mask", StringType, nullable = true),
      StructField("match", StringType, nullable = true),
      StructField("outiface", StringType, nullable = true),
      StructField("outiface_mask", StringType, nullable = true),
      StructField("packets", IntegerType, nullable = true),
      StructField("policy", StringType, nullable = true),
      StructField("protocol", IntegerType, nullable = true),
      StructField("src_ip", StringType, nullable = true),
      StructField("src_mask", StringType, nullable = true),
      StructField("src_port", StringType, nullable = true),
      StructField("target", StringType, nullable = true)
    )

    StructType(fields)

  }

  def kernel_extensions(): StructType = {

    val fields = Array(
      StructField("idx", IntegerType, nullable = true),
      StructField("linked_against", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("refs", IntegerType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def kernel_info(): StructType = {

    val fields = Array(
      StructField("arguments", StringType, nullable = true),
      StructField("device", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def kernel_modules(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("used_by", StringType, nullable = true)
    )

    StructType(fields)

  }

  def kernel_panics(): StructType = {

    val fields = Array(
      StructField("dependencies", StringType, nullable = true),
      StructField("frame_backtrace", StringType, nullable = true),
      StructField("kernel_version", StringType, nullable = true),
      StructField("last_loaded", StringType, nullable = true),
      StructField("last_unloaded", StringType, nullable = true),
      StructField("module_backtrace", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("os_version", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("registers", StringType, nullable = true),
      StructField("system_model", StringType, nullable = true),
      StructField("time", StringType, nullable = true),
      StructField("uptime", LongType, nullable = true)
    )

    StructType(fields)

  }

  def keychain_acls(): StructType = {

    val fields = Array(
      StructField("authorizations", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("keychain_path", StringType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def keychain_items(): StructType = {

    val fields = Array(
      StructField("comment", StringType, nullable = true),
      StructField("created", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("modified", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def known_hosts(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("key_file", StringType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def kva_speculative_info(): StructType = {

    val fields = Array(
      StructField("bp_microcode_disabled", IntegerType, nullable = true),
      StructField("bp_mitigations", IntegerType, nullable = true),
      StructField("bp_system_pol_disabled", IntegerType, nullable = true),
      StructField("cpu_pred_cmd_supported", IntegerType, nullable = true),
      StructField("cpu_spec_ctrl_supported", IntegerType, nullable = true),
      StructField("ibrs_support_enabled", IntegerType, nullable = true),
      StructField("kva_shadow_enabled", IntegerType, nullable = true),
      StructField("kva_shadow_inv_pcid", IntegerType, nullable = true),
      StructField("kva_shadow_pcid", IntegerType, nullable = true),
      StructField("kva_shadow_user_global", IntegerType, nullable = true),
      StructField("stibp_support_enabled", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def last(): StructType = {

    val fields = Array(
      StructField("host", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("time", IntegerType, nullable = true),
      StructField("tty", StringType, nullable = true),
      StructField("type", IntegerType, nullable = true),
      StructField("username", StringType, nullable = true)
    )

    StructType(fields)

  }

  def launchd(): StructType = {

    val fields = Array(
      StructField("disabled", StringType, nullable = true),
      StructField("groupname", StringType, nullable = true),
      StructField("inetd_compatibility", StringType, nullable = true),
      StructField("keep_alive", StringType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("on_demand", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("process_type", StringType, nullable = true),
      StructField("program", StringType, nullable = true),
      StructField("program_arguments", StringType, nullable = true),
      StructField("queue_directories", StringType, nullable = true),
      StructField("root_directory", StringType, nullable = true),
      StructField("run_at_load", StringType, nullable = true),
      StructField("start_interval", StringType, nullable = true),
      StructField("start_on_mount", StringType, nullable = true),
      StructField("stderr_path", StringType, nullable = true),
      StructField("stdout_path", StringType, nullable = true),
      StructField("username", StringType, nullable = true),
      StructField("watch_paths", StringType, nullable = true),
      StructField("working_directory", StringType, nullable = true)
    )

    StructType(fields)

  }

  def launchd_overrides(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def listening_ports(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("family", IntegerType, nullable = true),
      StructField("fd", LongType, nullable = true),
      StructField("net_namespace", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("port", IntegerType, nullable = true),
      StructField("protocol", IntegerType, nullable = true),
      StructField("socket", LongType, nullable = true)
    )

    StructType(fields)

  }

  def lldp_neighbors(): StructType = {

    val fields = Array(
      StructField("chassis_bridge_capability_available", IntegerType, nullable = true),
      StructField("chassis_bridge_capability_enabled", IntegerType, nullable = true),
      StructField("chassis_docsis_capability_available", IntegerType, nullable = true),
      StructField("chassis_docsis_capability_enabled", IntegerType, nullable = true),
      StructField("chassis_id", StringType, nullable = true),
      StructField("chassis_id_type", StringType, nullable = true),
      StructField("chassis_mgmt_ips", StringType, nullable = true),
      StructField("chassis_other_capability_available", IntegerType, nullable = true),
      StructField("chassis_other_capability_enabled", IntegerType, nullable = true),
      StructField("chassis_repeater_capability_available", IntegerType, nullable = true),
      StructField("chassis_repeater_capability_enabled", IntegerType, nullable = true),
      StructField("chassis_router_capability_available", IntegerType, nullable = true),
      StructField("chassis_router_capability_enabled", IntegerType, nullable = true),
      StructField("chassis_station_capability_available", IntegerType, nullable = true),
      StructField("chassis_station_capability_enabled", IntegerType, nullable = true),
      StructField("chassis_sys_description", IntegerType, nullable = true),
      StructField("chassis_sysname", StringType, nullable = true),
      StructField("chassis_tel_capability_available", IntegerType, nullable = true),
      StructField("chassis_tel_capability_enabled", IntegerType, nullable = true),
      StructField("chassis_wlan_capability_available", IntegerType, nullable = true),
      StructField("chassis_wlan_capability_enabled", IntegerType, nullable = true),
      StructField("interface", StringType, nullable = true),
      StructField("med_capability_capabilities", IntegerType, nullable = true),
      StructField("med_capability_inventory", IntegerType, nullable = true),
      StructField("med_capability_location", IntegerType, nullable = true),
      StructField("med_capability_mdi_pd", IntegerType, nullable = true),
      StructField("med_capability_mdi_pse", IntegerType, nullable = true),
      StructField("med_capability_policy", IntegerType, nullable = true),
      StructField("med_device_type", StringType, nullable = true),
      StructField("med_policies", StringType, nullable = true),
      StructField("pids", StringType, nullable = true),
      StructField("port_aggregation_id", StringType, nullable = true),
      StructField("port_autoneg_1000baset_fd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_1000baset_hd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_1000basex_fd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_1000basex_hd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_100baset2_fd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_100baset2_hd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_100baset4_fd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_100baset4_hd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_100basetx_fd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_100basetx_hd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_10baset_fd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_10baset_hd_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_enabled", IntegerType, nullable = true),
      StructField("port_autoneg_supported", IntegerType, nullable = true),
      StructField("port_description", StringType, nullable = true),
      StructField("port_id", StringType, nullable = true),
      StructField("port_id_type", StringType, nullable = true),
      StructField("port_mau_type", StringType, nullable = true),
      StructField("port_mfs", LongType, nullable = true),
      StructField("port_ttl", LongType, nullable = true),
      StructField("power_8023at_enabled", IntegerType, nullable = true),
      StructField("power_8023at_power_allocated", StringType, nullable = true),
      StructField("power_8023at_power_priority", StringType, nullable = true),
      StructField("power_8023at_power_requested", StringType, nullable = true),
      StructField("power_8023at_power_source", StringType, nullable = true),
      StructField("power_8023at_power_type", StringType, nullable = true),
      StructField("power_class", StringType, nullable = true),
      StructField("power_device_type", StringType, nullable = true),
      StructField("power_mdi_enabled", IntegerType, nullable = true),
      StructField("power_mdi_supported", IntegerType, nullable = true),
      StructField("power_paircontrol_enabled", IntegerType, nullable = true),
      StructField("power_pairs", StringType, nullable = true),
      StructField("ppvids_enabled", StringType, nullable = true),
      StructField("ppvids_supported", StringType, nullable = true),
      StructField("pvid", StringType, nullable = true),
      StructField("rid", IntegerType, nullable = true),
      StructField("vlans", StringType, nullable = true)
    )

    StructType(fields)

  }

  def load_average(): StructType = {

    val fields = Array(
      StructField("average", StringType, nullable = true),
      StructField("period", StringType, nullable = true)
    )

    StructType(fields)

  }

  def location_services(): StructType = {

    val fields = Array(
      StructField("enabled", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def logged_in_users(): StructType = {

    val fields = Array(
      StructField("host", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("registry_hive", StringType, nullable = true),
      StructField("sid", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("tty", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("user", StringType, nullable = true)
    )

    StructType(fields)

  }

  def logical_drives(): StructType = {

    val fields = Array(
      StructField("boot_partition", IntegerType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("device_id", StringType, nullable = true),
      StructField("file_system", StringType, nullable = true),
      StructField("free_space", LongType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def logon_sessions(): StructType = {

    val fields = Array(
      StructField("authentication_package", StringType, nullable = true),
      StructField("dns_domain_name", StringType, nullable = true),
      StructField("home_directory", StringType, nullable = true),
      StructField("home_directory_drive", StringType, nullable = true),
      StructField("logon_domain", StringType, nullable = true),
      StructField("logon_id", IntegerType, nullable = true),
      StructField("logon_script", StringType, nullable = true),
      StructField("logon_server", StringType, nullable = true),
      StructField("logon_sid", StringType, nullable = true),
      StructField("logon_time", LongType, nullable = true),
      StructField("logon_type", StringType, nullable = true),
      StructField("profile_path", StringType, nullable = true),
      StructField("session_id", IntegerType, nullable = true),
      StructField("upn", StringType, nullable = true),
      StructField("user", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_certificates(): StructType = {

    val fields = Array(
      StructField("certificate", StringType, nullable = true),
      StructField("fingerprint", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_cluster(): StructType = {

    val fields = Array(
      StructField("enabled", IntegerType, nullable = true),
      StructField("member_config_description", StringType, nullable = true),
      StructField("member_config_entity", StringType, nullable = true),
      StructField("member_config_key", StringType, nullable = true),
      StructField("member_config_name", StringType, nullable = true),
      StructField("member_config_value", StringType, nullable = true),
      StructField("server_name", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_cluster_members(): StructType = {

    val fields = Array(
      StructField("database", IntegerType, nullable = true),
      StructField("message", StringType, nullable = true),
      StructField("server_name", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("url", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_images(): StructType = {

    val fields = Array(
      StructField("aliases", StringType, nullable = true),
      StructField("architecture", StringType, nullable = true),
      StructField("auto_update", IntegerType, nullable = true),
      StructField("cached", IntegerType, nullable = true),
      StructField("created_at", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("expires_at", StringType, nullable = true),
      StructField("filename", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("last_used_at", StringType, nullable = true),
      StructField("os", StringType, nullable = true),
      StructField("public", IntegerType, nullable = true),
      StructField("release", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("update_source_alias", StringType, nullable = true),
      StructField("update_source_certificate", StringType, nullable = true),
      StructField("update_source_protocol", StringType, nullable = true),
      StructField("update_source_server", StringType, nullable = true),
      StructField("uploaded_at", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_instance_config(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_instance_devices(): StructType = {

    val fields = Array(
      StructField("device", StringType, nullable = true),
      StructField("device_type", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_instances(): StructType = {

    val fields = Array(
      StructField("architecture", StringType, nullable = true),
      StructField("base_image", StringType, nullable = true),
      StructField("created_at", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("ephemeral", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("os", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("processes", IntegerType, nullable = true),
      StructField("stateful", IntegerType, nullable = true),
      StructField("status", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_networks(): StructType = {

    val fields = Array(
      StructField("bytes_received", LongType, nullable = true),
      StructField("bytes_sent", LongType, nullable = true),
      StructField("hwaddr", StringType, nullable = true),
      StructField("ipv4_address", StringType, nullable = true),
      StructField("ipv6_address", StringType, nullable = true),
      StructField("managed", IntegerType, nullable = true),
      StructField("mtu", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("packets_received", LongType, nullable = true),
      StructField("packets_sent", LongType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("used_by", StringType, nullable = true)
    )

    StructType(fields)

  }

  def lxd_storage_pools(): StructType = {

    val fields = Array(
      StructField("driver", StringType, nullable = true),
      StructField("inodes_total", LongType, nullable = true),
      StructField("inodes_used", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("size", StringType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("space_total", LongType, nullable = true),
      StructField("space_used", LongType, nullable = true)
    )

    StructType(fields)

  }

  def magic(): StructType = {

    val fields = Array(
      StructField("data", StringType, nullable = true),
      StructField("magic_db_files", StringType, nullable = true),
      StructField("mime_encoding", StringType, nullable = true),
      StructField("mime_type", StringType, nullable = true),
      StructField("path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def managed_policies(): StructType = {

    val fields = Array(
      StructField("domain", StringType, nullable = true),
      StructField("manual", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("username", StringType, nullable = true),
      StructField("uuid", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def md_devices(): StructType = {

    val fields = Array(
      StructField("active_disks", IntegerType, nullable = true),
      StructField("bitmap_chunk_size", StringType, nullable = true),
      StructField("bitmap_external_file", StringType, nullable = true),
      StructField("bitmap_on_mem", StringType, nullable = true),
      StructField("check_array_finish", StringType, nullable = true),
      StructField("check_array_progress", StringType, nullable = true),
      StructField("check_array_speed", StringType, nullable = true),
      StructField("chunk_size", LongType, nullable = true),
      StructField("device_name", StringType, nullable = true),
      StructField("failed_disks", IntegerType, nullable = true),
      StructField("nr_raid_disks", IntegerType, nullable = true),
      StructField("other", StringType, nullable = true),
      StructField("raid_disks", IntegerType, nullable = true),
      StructField("raid_level", IntegerType, nullable = true),
      StructField("recovery_finish", StringType, nullable = true),
      StructField("recovery_progress", StringType, nullable = true),
      StructField("recovery_speed", StringType, nullable = true),
      StructField("reshape_finish", StringType, nullable = true),
      StructField("reshape_progress", StringType, nullable = true),
      StructField("reshape_speed", StringType, nullable = true),
      StructField("resync_finish", StringType, nullable = true),
      StructField("resync_progress", StringType, nullable = true),
      StructField("resync_speed", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("spare_disks", IntegerType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("superblock_state", StringType, nullable = true),
      StructField("superblock_update_time", LongType, nullable = true),
      StructField("superblock_version", StringType, nullable = true),
      StructField("unused_devices", StringType, nullable = true),
      StructField("working_disks", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def md_drives(): StructType = {

    val fields = Array(
      StructField("drive_name", StringType, nullable = true),
      StructField("md_device_name", StringType, nullable = true),
      StructField("slot", IntegerType, nullable = true),
      StructField("state", StringType, nullable = true)
    )

    StructType(fields)

  }

  def md_personalities(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true)
    )

    StructType(fields)

  }

  def mdfind(): StructType = {

    val fields = Array(
      StructField("path", StringType, nullable = true),
      StructField("query", StringType, nullable = true)
    )

    StructType(fields)

  }

  def mdls(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("value", StringType, nullable = true),
      StructField("valuetype", StringType, nullable = true)
    )

    StructType(fields)

  }

  def memory_array_mapped_addresses(): StructType = {

    val fields = Array(
      StructField("ending_address", StringType, nullable = true),
      StructField("handle", StringType, nullable = true),
      StructField("memory_array_handle", StringType, nullable = true),
      StructField("partition_width", IntegerType, nullable = true),
      StructField("starting_address", StringType, nullable = true)
    )

    StructType(fields)

  }

  def memory_arrays(): StructType = {

    val fields = Array(
      StructField("handle", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("max_capacity", IntegerType, nullable = true),
      StructField("memory_error_correction", StringType, nullable = true),
      StructField("memory_error_info_handle", StringType, nullable = true),
      StructField("number_memory_devices", IntegerType, nullable = true),
      StructField("use", StringType, nullable = true)
    )

    StructType(fields)

  }

  def memory_device_mapped_addresses(): StructType = {

    val fields = Array(
      StructField("ending_address", StringType, nullable = true),
      StructField("handle", StringType, nullable = true),
      StructField("interleave_data_depth", IntegerType, nullable = true),
      StructField("interleave_position", IntegerType, nullable = true),
      StructField("memory_array_mapped_address_handle", StringType, nullable = true),
      StructField("memory_device_handle", StringType, nullable = true),
      StructField("partition_row_position", IntegerType, nullable = true),
      StructField("starting_address", StringType, nullable = true)
    )

    StructType(fields)

  }

  def memory_devices(): StructType = {

    val fields = Array(
      StructField("array_handle", StringType, nullable = true),
      StructField("asset_tag", StringType, nullable = true),
      StructField("bank_locator", StringType, nullable = true),
      StructField("configured_clock_speed", IntegerType, nullable = true),
      StructField("configured_voltage", IntegerType, nullable = true),
      StructField("data_width", IntegerType, nullable = true),
      StructField("device_locator", StringType, nullable = true),
      StructField("form_factor", StringType, nullable = true),
      StructField("handle", StringType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("max_speed", IntegerType, nullable = true),
      StructField("max_voltage", IntegerType, nullable = true),
      StructField("memory_type", StringType, nullable = true),
      StructField("memory_type_details", StringType, nullable = true),
      StructField("min_voltage", IntegerType, nullable = true),
      StructField("part_number", StringType, nullable = true),
      StructField("serial_number", StringType, nullable = true),
      StructField("set", IntegerType, nullable = true),
      StructField("size", IntegerType, nullable = true),
      StructField("total_width", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def memory_error_info(): StructType = {

    val fields = Array(
      StructField("device_error_address", StringType, nullable = true),
      StructField("error_granularity", StringType, nullable = true),
      StructField("error_operation", StringType, nullable = true),
      StructField("error_resolution", StringType, nullable = true),
      StructField("error_type", StringType, nullable = true),
      StructField("handle", StringType, nullable = true),
      StructField("memory_array_error_address", StringType, nullable = true),
      StructField("vendor_syndrome", StringType, nullable = true)
    )

    StructType(fields)

  }

  def memory_info(): StructType = {

    val fields = Array(
      StructField("active", LongType, nullable = true),
      StructField("buffers", LongType, nullable = true),
      StructField("cached", LongType, nullable = true),
      StructField("inactive", LongType, nullable = true),
      StructField("memory_free", LongType, nullable = true),
      StructField("memory_total", LongType, nullable = true),
      StructField("swap_cached", LongType, nullable = true),
      StructField("swap_free", LongType, nullable = true),
      StructField("swap_total", LongType, nullable = true)
    )

    StructType(fields)

  }

  def memory_map(): StructType = {

    val fields = Array(
      StructField("end", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("start", StringType, nullable = true)
    )

    StructType(fields)

  }

  def mounts(): StructType = {

    val fields = Array(
      StructField("blocks", LongType, nullable = true),
      StructField("blocks_available", LongType, nullable = true),
      StructField("blocks_free", LongType, nullable = true),
      StructField("blocks_size", LongType, nullable = true),
      StructField("device", StringType, nullable = true),
      StructField("device_alias", StringType, nullable = true),
      StructField("flags", StringType, nullable = true),
      StructField("inodes", LongType, nullable = true),
      StructField("inodes_free", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def msr(): StructType = {

    val fields = Array(
      StructField("feature_control", LongType, nullable = true),
      StructField("perf_ctl", LongType, nullable = true),
      StructField("perf_status", LongType, nullable = true),
      StructField("platform_info", LongType, nullable = true),
      StructField("processor_number", LongType, nullable = true),
      StructField("rapl_energy_status", LongType, nullable = true),
      StructField("rapl_power_limit", LongType, nullable = true),
      StructField("rapl_power_units", LongType, nullable = true),
      StructField("turbo_disabled", LongType, nullable = true),
      StructField("turbo_ratio_limit", LongType, nullable = true)
    )

    StructType(fields)

  }

  def nfs_shares(): StructType = {

    val fields = Array(
      StructField("options", StringType, nullable = true),
      StructField("readonly", IntegerType, nullable = true),
      StructField("share", StringType, nullable = true)
    )

    StructType(fields)

  }

  def npm_packages(): StructType = {

    val fields = Array(
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("directory", StringType, nullable = true),
      StructField("license", StringType, nullable = true),
      StructField("mount_namespace_id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid_with_namespace", IntegerType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def ntdomains(): StructType = {

    val fields = Array(
      StructField("client_site_name", StringType, nullable = true),
      StructField("dc_site_name", StringType, nullable = true),
      StructField("dns_forest_name", StringType, nullable = true),
      StructField("domain_controller_address", StringType, nullable = true),
      StructField("domain_controller_name", StringType, nullable = true),
      StructField("domain_name", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("status", StringType, nullable = true)
    )

    StructType(fields)

  }

  def ntfs_acl_permissions(): StructType = {

    val fields = Array(
      StructField("access", StringType, nullable = true),
      StructField("inherited_from", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("principal", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def ntfs_journal_events(): StructType = {

    val fields = Array(
      StructField("action", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("drive_letter", StringType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("file_attributes", StringType, nullable = true),
      StructField("node_ref_number", StringType, nullable = true),
      StructField("old_path", StringType, nullable = true),
      StructField("parent_ref_number", StringType, nullable = true),
      StructField("partial", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("record_timestamp", StringType, nullable = true),
      StructField("record_usn", StringType, nullable = true),
      StructField("time", LongType, nullable = true)
    )

    StructType(fields)

  }

  def nvram(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def oem_strings(): StructType = {

    val fields = Array(
      StructField("handle", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def office_mru(): StructType = {

    val fields = Array(
      StructField("application", StringType, nullable = true),
      StructField("last_opened_time", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sid", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def opera_extensions(): StructType = {

    val fields = Array(
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("locale", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("persistent", IntegerType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("update_url", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def os_version(): StructType = {

    val fields = Array(
      StructField("arch", StringType, nullable = true),
      StructField("build", StringType, nullable = true),
      StructField("codename", StringType, nullable = true),
      StructField("install_date", LongType, nullable = true),
      StructField("major", IntegerType, nullable = true),
      StructField("minor", IntegerType, nullable = true),
      StructField("mount_namespace_id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("patch", IntegerType, nullable = true),
      StructField("pid_with_namespace", IntegerType, nullable = true),
      StructField("platform", StringType, nullable = true),
      StructField("platform_like", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def osquery_events(): StructType = {

    val fields = Array(
      StructField("active", IntegerType, nullable = true),
      StructField("events", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("publisher", StringType, nullable = true),
      StructField("refreshes", IntegerType, nullable = true),
      StructField("subscriptions", IntegerType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def osquery_extensions(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sdk_version", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uuid", LongType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def osquery_flags(): StructType = {

    val fields = Array(
      StructField("default_value", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("shell_only", IntegerType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def osquery_info(): StructType = {

    val fields = Array(
      StructField("build_distro", StringType, nullable = true),
      StructField("build_platform", StringType, nullable = true),
      StructField("config_hash", StringType, nullable = true),
      StructField("config_valid", IntegerType, nullable = true),
      StructField("extensions", StringType, nullable = true),
      StructField("instance_id", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("platform_mask", IntegerType, nullable = true),
      StructField("start_time", IntegerType, nullable = true),
      StructField("uuid", StringType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("watcher", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def osquery_packs(): StructType = {

    val fields = Array(
      StructField("active", IntegerType, nullable = true),
      StructField("discovery_cache_hits", IntegerType, nullable = true),
      StructField("discovery_executions", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("platform", StringType, nullable = true),
      StructField("shard", IntegerType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def osquery_registry(): StructType = {

    val fields = Array(
      StructField("active", IntegerType, nullable = true),
      StructField("internal", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("owner_uuid", IntegerType, nullable = true),
      StructField("registry", StringType, nullable = true)
    )

    StructType(fields)

  }

  def osquery_schedule(): StructType = {

    val fields = Array(
      StructField("average_memory", LongType, nullable = true),
      StructField("denylisted", IntegerType, nullable = true),
      StructField("executions", LongType, nullable = true),
      StructField("interval", IntegerType, nullable = true),
      StructField("last_executed", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("output_size", LongType, nullable = true),
      StructField("query", StringType, nullable = true),
      StructField("system_time", LongType, nullable = true),
      StructField("user_time", LongType, nullable = true),
      StructField("wall_time", LongType, nullable = true)
    )

    StructType(fields)

  }

  def package_bom(): StructType = {

    val fields = Array(
      StructField("filepath", StringType, nullable = true),
      StructField("gid", IntegerType, nullable = true),
      StructField("mode", IntegerType, nullable = true),
      StructField("modified_time", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("uid", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def package_install_history(): StructType = {

    val fields = Array(
      StructField("content_type", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("package_id", StringType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("time", IntegerType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def package_receipts(): StructType = {

    val fields = Array(
      StructField("install_time", DoubleType, nullable = true),
      StructField("installer_name", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("package_filename", StringType, nullable = true),
      StructField("package_id", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def patches(): StructType = {

    val fields = Array(
      StructField("caption", StringType, nullable = true),
      StructField("csname", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("fix_comments", StringType, nullable = true),
      StructField("hotfix_id", StringType, nullable = true),
      StructField("install_date", StringType, nullable = true),
      StructField("installed_by", StringType, nullable = true),
      StructField("installed_on", StringType, nullable = true)
    )

    StructType(fields)

  }

  def pci_devices(): StructType = {

    val fields = Array(
      StructField("driver", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("model_id", StringType, nullable = true),
      StructField("pci_class", StringType, nullable = true),
      StructField("pci_class_id", StringType, nullable = true),
      StructField("pci_slot", StringType, nullable = true),
      StructField("pci_subclass", StringType, nullable = true),
      StructField("pci_subclass_id", StringType, nullable = true),
      StructField("subsystem_model", StringType, nullable = true),
      StructField("subsystem_model_id", StringType, nullable = true),
      StructField("subsystem_vendor", StringType, nullable = true),
      StructField("subsystem_vendor_id", StringType, nullable = true),
      StructField("vendor", StringType, nullable = true),
      StructField("vendor_id", StringType, nullable = true)
    )

    StructType(fields)

  }

  def physical_disk_performance(): StructType = {

    val fields = Array(
      StructField("avg_disk_bytes_per_read", LongType, nullable = true),
      StructField("avg_disk_bytes_per_write", LongType, nullable = true),
      StructField("avg_disk_read_queue_length", LongType, nullable = true),
      StructField("avg_disk_sec_per_read", IntegerType, nullable = true),
      StructField("avg_disk_sec_per_write", IntegerType, nullable = true),
      StructField("avg_disk_write_queue_length", LongType, nullable = true),
      StructField("current_disk_queue_length", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("percent_disk_read_time", LongType, nullable = true),
      StructField("percent_disk_time", LongType, nullable = true),
      StructField("percent_disk_write_time", LongType, nullable = true),
      StructField("percent_idle_time", LongType, nullable = true)
    )

    StructType(fields)

  }

  def pipes(): StructType = {

    val fields = Array(
      StructField("flags", StringType, nullable = true),
      StructField("instances", IntegerType, nullable = true),
      StructField("max_instances", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("pid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def pkg_packages(): StructType = {

    val fields = Array(
      StructField("arch", StringType, nullable = true),
      StructField("flatsize", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def platform_info(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("date", StringType, nullable = true),
      StructField("extra", StringType, nullable = true),
      StructField("revision", StringType, nullable = true),
      StructField("size", StringType, nullable = true),
      StructField("vendor", StringType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("volume_size", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def plist(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("subkey", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def portage_keywords(): StructType = {

    val fields = Array(
      StructField("keyword", StringType, nullable = true),
      StructField("mask", IntegerType, nullable = true),
      StructField("package", StringType, nullable = true),
      StructField("unmask", IntegerType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def portage_packages(): StructType = {

    val fields = Array(
      StructField("build_time", LongType, nullable = true),
      StructField("eapi", LongType, nullable = true),
      StructField("package", StringType, nullable = true),
      StructField("repository", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("slot", StringType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("world", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def portage_use(): StructType = {

    val fields = Array(
      StructField("package", StringType, nullable = true),
      StructField("use", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def power_sensors(): StructType = {

    val fields = Array(
      StructField("category", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def powershell_events(): StructType = {

    val fields = Array(
      StructField("cosine_similarity", DoubleType, nullable = true),
      StructField("datetime", StringType, nullable = true),
      StructField("script_block_count", IntegerType, nullable = true),
      StructField("script_block_id", StringType, nullable = true),
      StructField("script_name", StringType, nullable = true),
      StructField("script_path", StringType, nullable = true),
      StructField("script_text", StringType, nullable = true),
      StructField("time", LongType, nullable = true)
    )

    StructType(fields)

  }

  def preferences(): StructType = {

    val fields = Array(
      StructField("domain", StringType, nullable = true),
      StructField("forced", IntegerType, nullable = true),
      StructField("host", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("subkey", StringType, nullable = true),
      StructField("username", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def process_envs(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def process_events(): StructType = {

    val fields = Array(
      StructField("atime", LongType, nullable = true),
      StructField("auid", LongType, nullable = true),
      StructField("btime", LongType, nullable = true),
      StructField("cmdline", StringType, nullable = true),
      StructField("cmdline_size", LongType, nullable = true),
      StructField("ctime", LongType, nullable = true),
      StructField("cwd", StringType, nullable = true),
      StructField("egid", LongType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("env", StringType, nullable = true),
      StructField("env_count", LongType, nullable = true),
      StructField("env_size", LongType, nullable = true),
      StructField("euid", LongType, nullable = true),
      StructField("fsgid", LongType, nullable = true),
      StructField("fsuid", LongType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("mtime", LongType, nullable = true),
      StructField("overflows", StringType, nullable = true),
      StructField("owner_gid", LongType, nullable = true),
      StructField("owner_uid", LongType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("sgid", LongType, nullable = true),
      StructField("status", LongType, nullable = true),
      StructField("suid", LongType, nullable = true),
      StructField("syscall", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("uptime", LongType, nullable = true)
    )

    StructType(fields)

  }

  def process_file_events(): StructType = {

    val fields = Array(
      StructField("auid", StringType, nullable = true),
      StructField("cwd", StringType, nullable = true),
      StructField("dest_path", StringType, nullable = true),
      StructField("egid", StringType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("euid", StringType, nullable = true),
      StructField("executable", StringType, nullable = true),
      StructField("fsgid", StringType, nullable = true),
      StructField("fsuid", StringType, nullable = true),
      StructField("gid", StringType, nullable = true),
      StructField("operation", StringType, nullable = true),
      StructField("partial", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("ppid", LongType, nullable = true),
      StructField("sgid", StringType, nullable = true),
      StructField("suid", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("uid", StringType, nullable = true),
      StructField("uptime", LongType, nullable = true)
    )

    StructType(fields)

  }

  def process_memory_map(): StructType = {

    val fields = Array(
      StructField("device", StringType, nullable = true),
      StructField("end", StringType, nullable = true),
      StructField("inode", IntegerType, nullable = true),
      StructField("offset", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("permissions", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("pseudo", IntegerType, nullable = true),
      StructField("start", StringType, nullable = true)
    )

    StructType(fields)

  }

  def process_namespaces(): StructType = {

    val fields = Array(
      StructField("cgroup_namespace", StringType, nullable = true),
      StructField("ipc_namespace", StringType, nullable = true),
      StructField("mnt_namespace", StringType, nullable = true),
      StructField("net_namespace", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("pid_namespace", StringType, nullable = true),
      StructField("user_namespace", StringType, nullable = true),
      StructField("uts_namespace", StringType, nullable = true)
    )

    StructType(fields)

  }

  def process_open_files(): StructType = {

    val fields = Array(
      StructField("fd", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def process_open_pipes(): StructType = {

    val fields = Array(
      StructField("fd", LongType, nullable = true),
      StructField("inode", LongType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("partner_fd", LongType, nullable = true),
      StructField("partner_mode", StringType, nullable = true),
      StructField("partner_pid", LongType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def process_open_sockets(): StructType = {

    val fields = Array(
      StructField("family", IntegerType, nullable = true),
      StructField("fd", LongType, nullable = true),
      StructField("local_address", StringType, nullable = true),
      StructField("local_port", IntegerType, nullable = true),
      StructField("net_namespace", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("protocol", IntegerType, nullable = true),
      StructField("remote_address", StringType, nullable = true),
      StructField("remote_port", IntegerType, nullable = true),
      StructField("socket", LongType, nullable = true),
      StructField("state", StringType, nullable = true)
    )

    StructType(fields)

  }

  def processes(): StructType = {

    val fields = Array(
      StructField("cmdline", StringType, nullable = true),
      StructField("cpu_subtype", IntegerType, nullable = true),
      StructField("cpu_type", IntegerType, nullable = true),
      StructField("cwd", StringType, nullable = true),
      StructField("disk_bytes_read", LongType, nullable = true),
      StructField("disk_bytes_written", LongType, nullable = true),
      StructField("egid", LongType, nullable = true),
      StructField("elapsed_time", LongType, nullable = true),
      StructField("euid", LongType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("handle_count", LongType, nullable = true),
      StructField("is_elevated_token", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("nice", IntegerType, nullable = true),
      StructField("on_disk", IntegerType, nullable = true),
      StructField("parent", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("percent_processor_time", LongType, nullable = true),
      StructField("pgroup", LongType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("resident_size", LongType, nullable = true),
      StructField("root", StringType, nullable = true),
      StructField("sgid", LongType, nullable = true),
      StructField("start_time", LongType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("suid", LongType, nullable = true),
      StructField("system_time", LongType, nullable = true),
      StructField("threads", IntegerType, nullable = true),
      StructField("total_size", LongType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("upid", LongType, nullable = true),
      StructField("uppid", LongType, nullable = true),
      StructField("user_time", LongType, nullable = true),
      StructField("wired_size", LongType, nullable = true)
    )

    StructType(fields)

  }

  def programs(): StructType = {

    val fields = Array(
      StructField("identifying_number", StringType, nullable = true),
      StructField("install_date", StringType, nullable = true),
      StructField("install_location", StringType, nullable = true),
      StructField("install_source", StringType, nullable = true),
      StructField("language", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("publisher", StringType, nullable = true),
      StructField("uninstall_string", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def prometheus_metrics(): StructType = {

    val fields = Array(
      StructField("metric_name", StringType, nullable = true),
      StructField("metric_value", DoubleType, nullable = true),
      StructField("target_name", StringType, nullable = true),
      StructField("timestamp_ms", LongType, nullable = true)
    )

    StructType(fields)

  }

  def python_packages(): StructType = {

    val fields = Array(
      StructField("author", StringType, nullable = true),
      StructField("directory", StringType, nullable = true),
      StructField("license", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("summary", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def quicklook_cache(): StructType = {

    val fields = Array(
      StructField("cache_path", StringType, nullable = true),
      StructField("fs_id", StringType, nullable = true),
      StructField("hit_count", StringType, nullable = true),
      StructField("icon_mode", LongType, nullable = true),
      StructField("inode", IntegerType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("last_hit_date", IntegerType, nullable = true),
      StructField("mtime", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("rowid", IntegerType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("volume_id", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def registry(): StructType = {

    val fields = Array(
      StructField("data", StringType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("mtime", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def routes(): StructType = {

    val fields = Array(
      StructField("destination", StringType, nullable = true),
      StructField("flags", IntegerType, nullable = true),
      StructField("gateway", StringType, nullable = true),
      StructField("hopcount", IntegerType, nullable = true),
      StructField("interface", StringType, nullable = true),
      StructField("metric", IntegerType, nullable = true),
      StructField("mtu", IntegerType, nullable = true),
      StructField("netmask", IntegerType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def rpm_package_files(): StructType = {

    val fields = Array(
      StructField("groupname", StringType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("package", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sha256", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("username", StringType, nullable = true)
    )

    StructType(fields)

  }

  def rpm_packages(): StructType = {

    val fields = Array(
      StructField("arch", StringType, nullable = true),
      StructField("epoch", IntegerType, nullable = true),
      StructField("install_time", IntegerType, nullable = true),
      StructField("mount_namespace_id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("package_group", StringType, nullable = true),
      StructField("pid_with_namespace", IntegerType, nullable = true),
      StructField("release", StringType, nullable = true),
      StructField("sha1", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("vendor", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def running_apps(): StructType = {

    val fields = Array(
      StructField("bundle_identifier", StringType, nullable = true),
      StructField("is_active", IntegerType, nullable = true),
      StructField("pid", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def safari_extensions(): StructType = {

    val fields = Array(
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("developer_id", StringType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sdk", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("update_url", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def sandboxes(): StructType = {

    val fields = Array(
      StructField("build_id", StringType, nullable = true),
      StructField("bundle_path", StringType, nullable = true),
      StructField("enabled", IntegerType, nullable = true),
      StructField("label", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("user", StringType, nullable = true)
    )

    StructType(fields)

  }

  def scheduled_tasks(): StructType = {

    val fields = Array(
      StructField("action", StringType, nullable = true),
      StructField("enabled", IntegerType, nullable = true),
      StructField("hidden", IntegerType, nullable = true),
      StructField("last_run_code", StringType, nullable = true),
      StructField("last_run_message", StringType, nullable = true),
      StructField("last_run_time", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("next_run_time", LongType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("state", StringType, nullable = true)
    )

    StructType(fields)

  }

  def screenlock(): StructType = {

    val fields = Array(
      StructField("enabled", IntegerType, nullable = true),
      StructField("grace_period", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def selinux_events(): StructType = {

    val fields = Array(
      StructField("eid", StringType, nullable = true),
      StructField("message", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uptime", LongType, nullable = true)
    )

    StructType(fields)

  }

  def selinux_settings(): StructType = {

    val fields = Array(
      StructField("key", StringType, nullable = true),
      StructField("scope", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def services(): StructType = {

    val fields = Array(
      StructField("description", StringType, nullable = true),
      StructField("display_name", StringType, nullable = true),
      StructField("module_path", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("service_exit_code", IntegerType, nullable = true),
      StructField("service_type", StringType, nullable = true),
      StructField("start_type", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("user_account", StringType, nullable = true),
      StructField("win32_exit_code", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def shadow(): StructType = {

    val fields = Array(
      StructField("expire", LongType, nullable = true),
      StructField("flag", LongType, nullable = true),
      StructField("hash_alg", StringType, nullable = true),
      StructField("inactive", LongType, nullable = true),
      StructField("last_change", LongType, nullable = true),
      StructField("max", LongType, nullable = true),
      StructField("min", LongType, nullable = true),
      StructField("password_status", StringType, nullable = true),
      StructField("username", StringType, nullable = true),
      StructField("warning", LongType, nullable = true)
    )

    StructType(fields)

  }

  def shared_folders(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def shared_memory(): StructType = {

    val fields = Array(
      StructField("atime", LongType, nullable = true),
      StructField("attached", IntegerType, nullable = true),
      StructField("creator_pid", LongType, nullable = true),
      StructField("creator_uid", LongType, nullable = true),
      StructField("ctime", LongType, nullable = true),
      StructField("dtime", LongType, nullable = true),
      StructField("locked", IntegerType, nullable = true),
      StructField("owner_uid", LongType, nullable = true),
      StructField("permissions", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("shmid", IntegerType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("status", StringType, nullable = true)
    )

    StructType(fields)

  }

  def shared_resources(): StructType = {

    val fields = Array(
      StructField("allow_maximum", IntegerType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("install_date", StringType, nullable = true),
      StructField("maximum_allowed", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("type", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def sharing_preferences(): StructType = {

    val fields = Array(
      StructField("bluetooth_sharing", IntegerType, nullable = true),
      StructField("content_caching", IntegerType, nullable = true),
      StructField("disc_sharing", IntegerType, nullable = true),
      StructField("file_sharing", IntegerType, nullable = true),
      StructField("internet_sharing", IntegerType, nullable = true),
      StructField("printer_sharing", IntegerType, nullable = true),
      StructField("remote_apple_events", IntegerType, nullable = true),
      StructField("remote_login", IntegerType, nullable = true),
      StructField("remote_management", IntegerType, nullable = true),
      StructField("screen_sharing", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def shell_history(): StructType = {

    val fields = Array(
      StructField("command", StringType, nullable = true),
      StructField("history_file", StringType, nullable = true),
      StructField("time", IntegerType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def shellbags(): StructType = {

    val fields = Array(
      StructField("accessed_time", IntegerType, nullable = true),
      StructField("created_time", IntegerType, nullable = true),
      StructField("mft_entry", LongType, nullable = true),
      StructField("mft_sequence", IntegerType, nullable = true),
      StructField("modified_time", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sid", StringType, nullable = true),
      StructField("source", StringType, nullable = true)
    )

    StructType(fields)

  }

  def shimcache(): StructType = {

    val fields = Array(
      StructField("entry", IntegerType, nullable = true),
      StructField("execution_flag", IntegerType, nullable = true),
      StructField("modified_time", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def signature(): StructType = {

    val fields = Array(
      StructField("arch", StringType, nullable = true),
      StructField("authority", StringType, nullable = true),
      StructField("cdhash", StringType, nullable = true),
      StructField("hash_resources", IntegerType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("signed", IntegerType, nullable = true),
      StructField("team_identifier", StringType, nullable = true)
    )

    StructType(fields)

  }

  def sip_config(): StructType = {

    val fields = Array(
      StructField("config_flag", StringType, nullable = true),
      StructField("enabled", IntegerType, nullable = true),
      StructField("enabled_nvram", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def smart_drive_info(): StructType = {

    val fields = Array(
      StructField("additional_product_id", StringType, nullable = true),
      StructField("ata_version", StringType, nullable = true),
      StructField("device_model", StringType, nullable = true),
      StructField("device_name", StringType, nullable = true),
      StructField("disk_id", IntegerType, nullable = true),
      StructField("driver_type", StringType, nullable = true),
      StructField("firmware_version", StringType, nullable = true),
      StructField("form_factor", StringType, nullable = true),
      StructField("in_smartctl_db", IntegerType, nullable = true),
      StructField("lu_wwn_device_id", StringType, nullable = true),
      StructField("model_family", StringType, nullable = true),
      StructField("packet_device_type", StringType, nullable = true),
      StructField("power_mode", StringType, nullable = true),
      StructField("read_device_identity_failure", StringType, nullable = true),
      StructField("rotation_rate", StringType, nullable = true),
      StructField("sata_version", StringType, nullable = true),
      StructField("sector_sizes", StringType, nullable = true),
      StructField("serial_number", StringType, nullable = true),
      StructField("smart_enabled", StringType, nullable = true),
      StructField("smart_supported", StringType, nullable = true),
      StructField("transport_type", StringType, nullable = true),
      StructField("user_capacity", StringType, nullable = true),
      StructField("warnings", StringType, nullable = true)
    )

    StructType(fields)

  }

  def smbios_tables(): StructType = {

    val fields = Array(
      StructField("description", StringType, nullable = true),
      StructField("handle", IntegerType, nullable = true),
      StructField("header_size", IntegerType, nullable = true),
      StructField("md5", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true),
      StructField("size", IntegerType, nullable = true),
      StructField("type", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def smc_keys(): StructType = {

    val fields = Array(
      StructField("hidden", IntegerType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("size", IntegerType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def socket_events(): StructType = {

    val fields = Array(
      StructField("action", StringType, nullable = true),
      StructField("auid", LongType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("family", IntegerType, nullable = true),
      StructField("fd", StringType, nullable = true),
      StructField("local_address", StringType, nullable = true),
      StructField("local_port", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("protocol", IntegerType, nullable = true),
      StructField("remote_address", StringType, nullable = true),
      StructField("remote_port", IntegerType, nullable = true),
      StructField("socket", StringType, nullable = true),
      StructField("success", IntegerType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("uptime", LongType, nullable = true)
    )

    StructType(fields)

  }

  def ssh_configs(): StructType = {

    val fields = Array(
      StructField("block", StringType, nullable = true),
      StructField("option", StringType, nullable = true),
      StructField("ssh_config_file", StringType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def startup_items(): StructType = {

    val fields = Array(
      StructField("args", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("username", StringType, nullable = true)
    )

    StructType(fields)

  }

  def sudoers(): StructType = {

    val fields = Array(
      StructField("header", StringType, nullable = true),
      StructField("rule_details", StringType, nullable = true),
      StructField("source", StringType, nullable = true)
    )

    StructType(fields)

  }

  def suid_bin(): StructType = {

    val fields = Array(
      StructField("groupname", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("permissions", StringType, nullable = true),
      StructField("username", StringType, nullable = true)
    )

    StructType(fields)

  }

  def syslog_events(): StructType = {

    val fields = Array(
      StructField("datetime", StringType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("facility", StringType, nullable = true),
      StructField("host", StringType, nullable = true),
      StructField("message", StringType, nullable = true),
      StructField("severity", IntegerType, nullable = true),
      StructField("tag", StringType, nullable = true),
      StructField("time", LongType, nullable = true)
    )

    StructType(fields)

  }

  def system_controls(): StructType = {

    val fields = Array(
      StructField("config_value", StringType, nullable = true),
      StructField("current_value", StringType, nullable = true),
      StructField("field_name", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("oid", StringType, nullable = true),
      StructField("subsystem", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def system_extensions(): StructType = {

    val fields = Array(
      StructField("bundle_path", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("team", StringType, nullable = true),
      StructField("uuid", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def system_info(): StructType = {

    val fields = Array(
      StructField("board_model", StringType, nullable = true),
      StructField("board_serial", StringType, nullable = true),
      StructField("board_vendor", StringType, nullable = true),
      StructField("board_version", StringType, nullable = true),
      StructField("computer_name", StringType, nullable = true),
      StructField("cpu_brand", StringType, nullable = true),
      StructField("cpu_logical_cores", IntegerType, nullable = true),
      StructField("cpu_microcode", StringType, nullable = true),
      StructField("cpu_physical_cores", IntegerType, nullable = true),
      StructField("cpu_subtype", StringType, nullable = true),
      StructField("cpu_type", StringType, nullable = true),
      StructField("hardware_model", StringType, nullable = true),
      StructField("hardware_serial", StringType, nullable = true),
      StructField("hardware_vendor", StringType, nullable = true),
      StructField("hardware_version", StringType, nullable = true),
      StructField("hostname", StringType, nullable = true),
      StructField("local_hostname", StringType, nullable = true),
      StructField("physical_memory", LongType, nullable = true),
      StructField("uuid", StringType, nullable = true)
    )

    StructType(fields)

  }

  def systemd_units(): StructType = {

    val fields = Array(
      StructField("active_state", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("following", StringType, nullable = true),
      StructField("fragment_path", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("job_id", LongType, nullable = true),
      StructField("job_path", StringType, nullable = true),
      StructField("job_type", StringType, nullable = true),
      StructField("load_state", StringType, nullable = true),
      StructField("object_path", StringType, nullable = true),
      StructField("source_path", StringType, nullable = true),
      StructField("sub_state", StringType, nullable = true),
      StructField("user", StringType, nullable = true)
    )

    StructType(fields)

  }

  def temperature_sensors(): StructType = {

    val fields = Array(
      StructField("celsius", DoubleType, nullable = true),
      StructField("fahrenheit", DoubleType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("name", StringType, nullable = true)
    )

    StructType(fields)

  }

  def time(): StructType = {

    val fields = Array(
      StructField("datetime", StringType, nullable = true),
      StructField("day", IntegerType, nullable = true),
      StructField("hour", IntegerType, nullable = true),
      StructField("iso_8601", StringType, nullable = true),
      StructField("local_time", IntegerType, nullable = true),
      StructField("local_timezone", StringType, nullable = true),
      StructField("minutes", IntegerType, nullable = true),
      StructField("month", IntegerType, nullable = true),
      StructField("seconds", IntegerType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("timezone", StringType, nullable = true),
      StructField("unix_time", IntegerType, nullable = true),
      StructField("weekday", StringType, nullable = true),
      StructField("win_timestamp", LongType, nullable = true),
      StructField("year", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def time_machine_backups(): StructType = {

    val fields = Array(
      StructField("backup_date", IntegerType, nullable = true),
      StructField("destination_id", StringType, nullable = true)
    )

    StructType(fields)

  }

  def time_machine_destinations(): StructType = {

    val fields = Array(
      StructField("alias", StringType, nullable = true),
      StructField("bytes_available", IntegerType, nullable = true),
      StructField("bytes_used", IntegerType, nullable = true),
      StructField("consistency_scan_date", IntegerType, nullable = true),
      StructField("destination_id", StringType, nullable = true),
      StructField("encryption", StringType, nullable = true),
      StructField("root_volume_uuid", StringType, nullable = true)
    )

    StructType(fields)

  }

  def ulimit_info(): StructType = {

    val fields = Array(
      StructField("hard_limit", StringType, nullable = true),
      StructField("soft_limit", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def uptime(): StructType = {

    val fields = Array(
      StructField("days", IntegerType, nullable = true),
      StructField("hours", IntegerType, nullable = true),
      StructField("minutes", IntegerType, nullable = true),
      StructField("seconds", IntegerType, nullable = true),
      StructField("total_seconds", LongType, nullable = true)
    )

    StructType(fields)

  }

  def usb_devices(): StructType = {

    val fields = Array(
      StructField("class", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("model_id", StringType, nullable = true),
      StructField("protocol", StringType, nullable = true),
      StructField("removable", IntegerType, nullable = true),
      StructField("serial", StringType, nullable = true),
      StructField("subclass", StringType, nullable = true),
      StructField("usb_address", IntegerType, nullable = true),
      StructField("usb_port", IntegerType, nullable = true),
      StructField("vendor", StringType, nullable = true),
      StructField("vendor_id", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def user_events(): StructType = {

    val fields = Array(
      StructField("address", StringType, nullable = true),
      StructField("auid", LongType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("message", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("terminal", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("type", IntegerType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("uptime", LongType, nullable = true)
    )

    StructType(fields)

  }

  def user_groups(): StructType = {

    val fields = Array(
      StructField("gid", LongType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def user_interaction_events(): StructType = {

    val fields = Array(
      StructField("time", LongType, nullable = true)
    )

    StructType(fields)

  }

  def user_ssh_keys(): StructType = {

    val fields = Array(
      StructField("encrypted", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("uid", LongType, nullable = true)
    )

    StructType(fields)

  }

  def userassist(): StructType = {

    val fields = Array(
      StructField("count", IntegerType, nullable = true),
      StructField("last_execution_time", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sid", StringType, nullable = true)
    )

    StructType(fields)

  }

  def users(): StructType = {

    val fields = Array(
      StructField("description", StringType, nullable = true),
      StructField("directory", StringType, nullable = true),
      StructField("gid", LongType, nullable = true),
      StructField("gid_signed", LongType, nullable = true),
      StructField("is_hidden", IntegerType, nullable = true),
      StructField("shell", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("uid", LongType, nullable = true),
      StructField("uid_signed", LongType, nullable = true),
      StructField("username", StringType, nullable = true),
      StructField("uuid", StringType, nullable = true)
    )

    StructType(fields)

  }

  def video_info(): StructType = {

    val fields = Array(
      StructField("color_depth", IntegerType, nullable = true),
      StructField("driver", StringType, nullable = true),
      StructField("driver_date", LongType, nullable = true),
      StructField("driver_version", StringType, nullable = true),
      StructField("manufacturer", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("series", StringType, nullable = true),
      StructField("video_mode", StringType, nullable = true)
    )

    StructType(fields)

  }

  def virtual_memory_info(): StructType = {

    val fields = Array(
      StructField("active", LongType, nullable = true),
      StructField("anonymous", LongType, nullable = true),
      StructField("compressed", LongType, nullable = true),
      StructField("compressor", LongType, nullable = true),
      StructField("copy", LongType, nullable = true),
      StructField("decompressed", LongType, nullable = true),
      StructField("faults", LongType, nullable = true),
      StructField("file_backed", LongType, nullable = true),
      StructField("free", LongType, nullable = true),
      StructField("inactive", LongType, nullable = true),
      StructField("page_ins", LongType, nullable = true),
      StructField("page_outs", LongType, nullable = true),
      StructField("purgeable", LongType, nullable = true),
      StructField("purged", LongType, nullable = true),
      StructField("reactivated", LongType, nullable = true),
      StructField("speculative", LongType, nullable = true),
      StructField("swap_ins", LongType, nullable = true),
      StructField("swap_outs", LongType, nullable = true),
      StructField("throttled", LongType, nullable = true),
      StructField("uncompressed", LongType, nullable = true),
      StructField("wired", LongType, nullable = true),
      StructField("zero_fill", LongType, nullable = true)
    )

    StructType(fields)

  }

  def wifi_networks(): StructType = {

    val fields = Array(
      StructField("auto_login", IntegerType, nullable = true),
      StructField("captive_portal", IntegerType, nullable = true),
      StructField("disabled", IntegerType, nullable = true),
      StructField("last_connected", IntegerType, nullable = true),
      StructField("network_name", StringType, nullable = true),
      StructField("passpoint", IntegerType, nullable = true),
      StructField("possibly_hidden", IntegerType, nullable = true),
      StructField("roaming", IntegerType, nullable = true),
      StructField("roaming_profile", StringType, nullable = true),
      StructField("security_type", StringType, nullable = true),
      StructField("ssid", StringType, nullable = true),
      StructField("temporarily_disabled", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def wifi_status(): StructType = {

    val fields = Array(
      StructField("bssid", StringType, nullable = true),
      StructField("channel", IntegerType, nullable = true),
      StructField("channel_band", IntegerType, nullable = true),
      StructField("channel_width", IntegerType, nullable = true),
      StructField("country_code", StringType, nullable = true),
      StructField("interface", StringType, nullable = true),
      StructField("mode", StringType, nullable = true),
      StructField("network_name", StringType, nullable = true),
      StructField("noise", IntegerType, nullable = true),
      StructField("rssi", IntegerType, nullable = true),
      StructField("security_type", StringType, nullable = true),
      StructField("ssid", StringType, nullable = true),
      StructField("transmit_rate", StringType, nullable = true)
    )

    StructType(fields)

  }

  def wifi_survey(): StructType = {

    val fields = Array(
      StructField("bssid", StringType, nullable = true),
      StructField("channel", IntegerType, nullable = true),
      StructField("channel_band", IntegerType, nullable = true),
      StructField("channel_width", IntegerType, nullable = true),
      StructField("country_code", StringType, nullable = true),
      StructField("interface", StringType, nullable = true),
      StructField("network_name", StringType, nullable = true),
      StructField("noise", IntegerType, nullable = true),
      StructField("rssi", IntegerType, nullable = true),
      StructField("ssid", StringType, nullable = true)
    )

    StructType(fields)

  }

  def winbaseobj(): StructType = {

    val fields = Array(
      StructField("object_name", StringType, nullable = true),
      StructField("object_type", StringType, nullable = true),
      StructField("session_id", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def windows_crashes(): StructType = {

    val fields = Array(
      StructField("build_number", IntegerType, nullable = true),
      StructField("command_line", StringType, nullable = true),
      StructField("crash_path", StringType, nullable = true),
      StructField("current_directory", StringType, nullable = true),
      StructField("datetime", StringType, nullable = true),
      StructField("exception_address", StringType, nullable = true),
      StructField("exception_code", StringType, nullable = true),
      StructField("exception_message", StringType, nullable = true),
      StructField("machine_name", StringType, nullable = true),
      StructField("major_version", IntegerType, nullable = true),
      StructField("minor_version", IntegerType, nullable = true),
      StructField("module", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("pid", LongType, nullable = true),
      StructField("process_uptime", LongType, nullable = true),
      StructField("registers", StringType, nullable = true),
      StructField("stack_trace", StringType, nullable = true),
      StructField("tid", LongType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("username", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )

    StructType(fields)

  }

  def windows_eventlog(): StructType = {

    val fields = Array(
      StructField("channel", StringType, nullable = true),
      StructField("computer_name", StringType, nullable = true),
      StructField("data", StringType, nullable = true),
      StructField("datetime", StringType, nullable = true),
      StructField("eventid", IntegerType, nullable = true),
      StructField("keywords", StringType, nullable = true),
      StructField("level", IntegerType, nullable = true),
      StructField("pid", IntegerType, nullable = true),
      StructField("provider_guid", StringType, nullable = true),
      StructField("provider_name", StringType, nullable = true),
      StructField("task", IntegerType, nullable = true),
      StructField("tid", IntegerType, nullable = true),
      StructField("time_range", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("xpath", StringType, nullable = true)
    )

    StructType(fields)

  }

  def windows_events(): StructType = {

    val fields = Array(
      StructField("computer_name", StringType, nullable = true),
      StructField("data", StringType, nullable = true),
      StructField("datetime", StringType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("eventid", IntegerType, nullable = true),
      StructField("keywords", StringType, nullable = true),
      StructField("level", IntegerType, nullable = true),
      StructField("provider_guid", StringType, nullable = true),
      StructField("provider_name", StringType, nullable = true),
      StructField("source", StringType, nullable = true),
      StructField("task", IntegerType, nullable = true),
      StructField("time", LongType, nullable = true)
    )

    StructType(fields)

  }

  def windows_optional_features(): StructType = {

    val fields = Array(
      StructField("caption", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("state", IntegerType, nullable = true),
      StructField("statename", StringType, nullable = true)
    )

    StructType(fields)

  }

  def windows_security_center(): StructType = {

    val fields = Array(
      StructField("antispyware", StringType, nullable = true),
      StructField("antivirus", StringType, nullable = true),
      StructField("autoupdate", StringType, nullable = true),
      StructField("firewall", StringType, nullable = true),
      StructField("internet_settings", StringType, nullable = true),
      StructField("user_account_control", StringType, nullable = true),
      StructField("windows_security_center_service", StringType, nullable = true)
    )

    StructType(fields)

  }

  def windows_security_products(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("remediation_path", StringType, nullable = true),
      StructField("signatures_up_to_date", IntegerType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("state_timestamp", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def wmi_bios_info(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )

    StructType(fields)

  }

  def wmi_cli_event_consumers(): StructType = {

    val fields = Array(
      StructField("class", StringType, nullable = true),
      StructField("command_line_template", StringType, nullable = true),
      StructField("executable_path", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("relative_path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def wmi_event_filters(): StructType = {

    val fields = Array(
      StructField("class", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("query", StringType, nullable = true),
      StructField("query_language", StringType, nullable = true),
      StructField("relative_path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def wmi_filter_consumer_binding(): StructType = {

    val fields = Array(
      StructField("class", StringType, nullable = true),
      StructField("consumer", StringType, nullable = true),
      StructField("filter", StringType, nullable = true),
      StructField("relative_path", StringType, nullable = true)
    )

    StructType(fields)

  }

  def wmi_script_event_consumers(): StructType = {

    val fields = Array(
      StructField("class", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("relative_path", StringType, nullable = true),
      StructField("script_file_name", StringType, nullable = true),
      StructField("script_text", StringType, nullable = true),
      StructField("scripting_engine", StringType, nullable = true)
    )

    StructType(fields)

  }

  def xprotect_entries(): StructType = {

    val fields = Array(
      StructField("filename", StringType, nullable = true),
      StructField("filetype", StringType, nullable = true),
      StructField("identity", StringType, nullable = true),
      StructField("launch_type", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("optional", IntegerType, nullable = true),
      StructField("uses_pattern", IntegerType, nullable = true)
    )

    StructType(fields)

  }

  def xprotect_meta(): StructType = {

    val fields = Array(
      StructField("developer_id", StringType, nullable = true),
      StructField("identifier", StringType, nullable = true),
      StructField("min_version", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    StructType(fields)

  }

  def xprotect_reports(): StructType = {

    val fields = Array(
      StructField("name", StringType, nullable = true),
      StructField("time", StringType, nullable = true),
      StructField("user_action", StringType, nullable = true)
    )

    StructType(fields)

  }

  def yara(): StructType = {

    val fields = Array(
      StructField("count", IntegerType, nullable = true),
      StructField("matches", StringType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("sig_group", StringType, nullable = true),
      StructField("sigfile", StringType, nullable = true),
      StructField("sigrule", StringType, nullable = true),
      StructField("sigurl", StringType, nullable = true),
      StructField("strings", StringType, nullable = true),
      StructField("tags", StringType, nullable = true)
    )

    StructType(fields)

  }

  def yara_events(): StructType = {

    val fields = Array(
      StructField("action", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("count", IntegerType, nullable = true),
      StructField("eid", StringType, nullable = true),
      StructField("matches", StringType, nullable = true),
      StructField("strings", StringType, nullable = true),
      StructField("tags", StringType, nullable = true),
      StructField("target_path", StringType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("transaction_id", LongType, nullable = true)
    )

    StructType(fields)

  }

  def ycloud_instance_metadata(): StructType = {

    val fields = Array(
      StructField("description", StringType, nullable = true),
      StructField("folder_id", StringType, nullable = true),
      StructField("hostname", StringType, nullable = true),
      StructField("instance_id", StringType, nullable = true),
      StructField("metadata_endpoint", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("serial_port_enabled", StringType, nullable = true),
      StructField("ssh_public_key", StringType, nullable = true),
      StructField("zone", StringType, nullable = true)
    )

    StructType(fields)

  }

  def yum_sources(): StructType = {

    val fields = Array(
      StructField("baseurl", StringType, nullable = true),
      StructField("enabled", StringType, nullable = true),
      StructField("gpgcheck", StringType, nullable = true),
      StructField("gpgkey", StringType, nullable = true),
      StructField("name", StringType, nullable = true)
    )

    StructType(fields)

  }
  /*
   * The list of Osquery tables (version 4.6.0) is extended
   * by a proprietary `osquery_status` table to also take
   * status log event into account.
   *
   * Status events are represented by single column table
   * where the column contains the serialized message.
   *
   */
  def osquery_status(): StructType = {

    val fields = Array(
      StructField("message", StringType, nullable = false)
    )

    StructType(fields)

  }

}
