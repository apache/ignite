package org.apache.ignite.spi.discovery.tcp.ipfinder.zk;

import org.codehaus.jackson.map.annotate.JsonRootName;

/**
 * Empty DTO for storing service instances details. May be enhanced in the future
 * with further information to assist discovery.
 *
 * @author Raul Kripalani
 */
@JsonRootName("ignite_instance_details")
public class IgniteInstanceDetails {

}
