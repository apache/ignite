package de.kp.works.ignite.streamer.fiware
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.core.BaseSsl

object FiwareSsl extends BaseSsl {

  def isFiwareSsl: Boolean = {
    /*
      * Distinguish between SSL/TLS and non-SSL/TLS requests;
      * note, [IgniteConf] must be initialized.
      */
    val cfg = WorksConf.getFiwareBrokerSecurity
    if (cfg.getString("ssl") == "false") false else true
  }

  /**
   * Distinguish between SSL/TLS and non-SSL/TLS requests;
   * note, [IgniteConf] must be initialized.
   */
  def isServerSsl: Boolean = {
    val serverCfg = WorksConf.getServerCfg(WorksConf.FIWARE_CONF)
    val securityCfg = serverCfg.getConfig("security")

    if (securityCfg.getString("ssl") == "false") false else true
  }

  def buildBrokerContext: HttpsConnectionContext = {
    val cfg = WorksConf.getFiwareBrokerSecurity
    ConnectionContext.https(buildSSLContext(cfg))
  }

  def buildServerContext: HttpsConnectionContext = {

    val serverCfg = WorksConf.getServerCfg(WorksConf.FIWARE_CONF)
    val securityCfg = serverCfg.getConfig("security")

    ConnectionContext.https(buildSSLContext(securityCfg))
  }

}
