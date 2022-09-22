package de.kp.works.ignite.transform.zeek

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

/*
 * https://docs.zeek.org/en/master/script-reference/log-files.html
 */
object ZeekFormats extends Enumeration {

  type ZeekFormat = Value

  /* Diagnostics :: Packet loss rate
   */
  val CAPTURE_LOSS: ZeekFormats.Value = Value(1, "capture_loss.log")
  /* Network :: TCP/UDP/ICMP connections
   */
  val CONNECTION: ZeekFormats.Value = Value(2, "conn.log")
  /* Network :: Distributed Computing Environment/RPC
   *
   * SMB (Server Message Block) Log
   */
  val DCE_RPC: ZeekFormats.Value = Value(3, "dce_rpc.log")
  /* Network :: DHCP leases
   */
  val DHCP: ZeekFormats.Value = Value(4, "dhcp.log")
  /* Network :: DNP3 requests and replies
   */
  val DNP3: ZeekFormats.Value = Value(5, "dnp3.log")
  /* Network :: DNS activity
   */
  val DNS: ZeekFormats.Value = Value(6, "dns.log")
  /* Miscellaneous :: Dynamic protocol detection failures
   */
  val DPD: ZeekFormats.Value = Value(7, "dpd.log")
  /* Files :: File analysis results
   */
  val FILES: ZeekFormats.Value = Value(8, "files.log")
  /* Network :: FTP activity
   */
  val FTP: ZeekFormats.Value = Value(9, "ftp.log")
  /* Network :: HTTP requests and replies
   */
  val HTTP: ZeekFormats.Value = Value(10, "http.log")
  /* Detection :: Intelligence data matches
   */
  val INTEL: ZeekFormats.Value = Value(11, "intel.log")
  /* Network :: IRC commands and responses
   */
  val IRC: ZeekFormats.Value = Value(12, "irc.log")
  /* Network :: Kerberos
   *
   * SMB (Server Message Block) Log
   */
  val KERBEROS: ZeekFormats.Value = Value(13, "kerberos.log")
  /* Network :: Modbus commands and responses
   */
  val MODBUS: ZeekFormats.Value = Value(14, "modbus.log")
  /* Network :: MySQL
   */
  val MYSQL: ZeekFormats.Value = Value(15, "mysql.log")
  /* Detection :: Zeek notices
   *
   * SMB (Server Message Block) Log
   */
  val NOTICE: ZeekFormats.Value = Value(16, "notice.log")
  /* Network :: NT LAN Manager (NTLM)
   *
   * SMB (Server Message Block) Log
   */
  val NTLM: ZeekFormats.Value = Value(17, "ntlm.log")
  /* Files :: Online Certificate Status Protocol (OCSP).
   *
   * Only created if policy script is loaded.
   */
  val OCSP: ZeekFormats.Value = Value(18, "ocsp.log")
  /* Files :: Portable Executable (PE)
   *
   * SMB (Server Message Block) Log
   */
  val PE: ZeekFormats.Value = Value(19, "pe.log")
  /* Network :: RADIUS authentication attempts
   */
  val RADIUS: ZeekFormats.Value = Value(20, "radius.log")
  /* Network :: RDP
   */
  val RDP: ZeekFormats.Value = Value(21, "rdp.log")
  /* Network :: Remote Framebuffer (RFB)
   */
  val RFB: ZeekFormats.Value = Value(22, "rfb.log")
  /* Network :: SIP
   */
  val SIP: ZeekFormats.Value = Value(23, "sip.log")
  /* Network :: SMB commands
   *
   * SMB (Server Message Block) Log
   */
  val SMB_CMD: ZeekFormats.Value = Value(24, "smb_cmd.log")
  /* Network :: SMB files
   *
   * SMB (Server Message Block) Log
   */
  val SMB_FILES: ZeekFormats.Value = Value(25, "smb_files.log")
  /* Network :: SMB trees
   *
   * SMB (Server Message Block) Log
   */
  val SMB_MAPPING: ZeekFormats.Value = Value(26, "smb_mapping.log")
  /* Network :: SMTP transactions
   */
  val SMTP: ZeekFormats.Value = Value(27, "smtp.log")
  /* Network :: SNMP messages
   */
  val SNMP: ZeekFormats.Value = Value(28, "snmp.log")
  /* Network :: SOCKS proxy requests
   */
  val SOCKS: ZeekFormats.Value = Value(29, "socks.log")
  /* Network :: SSH connections
   */
  val SSH: ZeekFormats.Value = Value(30, "ssh.log")
  /* Network :: SSL/TLS handshake info
   */
  val SSL: ZeekFormats.Value = Value(31, "ssl.log")
  /* Diagnostics :: Memory/event/packet/lag statistics
   */
  val STATS: ZeekFormats.Value = Value(32, "stats.log")
  /* Network :: Syslog messages
   */
  val SYSLOG: ZeekFormats.Value = Value(33, "syslog.log")
  /* Detection :: Traceroute detection
   */
  val TRACEROUTE: ZeekFormats.Value   = Value(34, "traceroute.log")
  /* Network :: Tunneling protocol events
   */
  val TUNNEL: ZeekFormats.Value = Value(35, "tunnel.log")
  /* Miscellaneous :: Unexpected network-level activity
   */
  val WEIRD: ZeekFormats.Value = Value(36, "weird.log")
  /* Files :: X.509 certificate info
   */
  val X509: ZeekFormats.Value = Value(37, "x509.log")

}

object ZeekFormatUtil {

  def fromFile(fileName:String):ZeekFormats.Value = {

    val formats = ZeekFormats.values.filter(format => {
      fileName.contains(format.toString)
    })

    if (formats.isEmpty) return null
    formats.head

  }
}
