package de.kp.works.ignite.streamer.fiware.graph
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

import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.streamer.fiware.graph.transformer._

object FiwareModels extends Enumeration {
  val AgriFood: FiwareModels.Value = Value("AgriFood")
  val City: FiwareModels.Value     = Value("City")
  val Energy: FiwareModels.Value   = Value("Energy")
  val Industry: FiwareModels.Value = Value("Industry")
}
/**
 * The [FiwareGraphFactory] is responsible to transform
 * a specific Context Broker notification into IgniteGraph
 * PUT mutations.
 */
object FiwareGraphFactory {

  def getTransformer:FiwareTransformer = {

    val model = WorksConf.getFiwareDataModel
    val name = model.getString("name")

    FiwareModels.withName(name) match {
      case FiwareModels.AgriFood => FiwareAgriFood
      case FiwareModels.City     => FiwareCity
      case FiwareModels.Energy   => FiwareEnergy
      case FiwareModels.Industry => FiwareIndustry
      case _ => throw new Exception(s"The data model `$name` is not supported.")
    }

  }
}
