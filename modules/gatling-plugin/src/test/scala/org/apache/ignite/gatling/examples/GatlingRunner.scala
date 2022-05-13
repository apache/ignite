package org.apache.ignite.gatling.examples

import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder

object GatlingRunner {

    def main(args: Array[String]): Unit = {

        // this is where you specify the class you want to run
        val simulationClass = classOf[BasicSimulation].getName

        val props = new GatlingPropertiesBuilder
        props.simulationClass(simulationClass)

        Gatling.fromMap(props.build)
    }

}
