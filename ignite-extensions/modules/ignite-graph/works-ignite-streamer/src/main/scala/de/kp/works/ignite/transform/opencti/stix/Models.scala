package de.kp.works.ignite.transform.opencti.stix
/**
 * Copyright (c) 2020 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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

import java.sql.Date

case class Asset(
  `type`:String,
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  category:String,
  kind_of_asset:String,
  category_ext:List[Any],
  compromised:Boolean = false,
  owner_aware:Boolean = false,
  technical_characteristics:List[Any])

case class AttackPattern(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  aliases:List[Any],
  kill_chain_phases:List[Any])

case class Campaign(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  aliases:List[Any],
  first_seen:Date,
  last_seen:Date,
  objective:String)

case class CourseOfAction(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  action:String)

case class ExternalReference(
  source_name:String,
  description:String,
  url:String,
  hashes:Map[String,Any],
  external_id:String)

case class GranularMarking(
  lang:String,
  marking_ref:List[Any],
  selectors:List[Any]
)

case class Grouping(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  context:String,
  object_refs:List[Any])

case class Identity(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  identity_class:String,
  sectors:List[Any],
  contact_information:String)

case class Indicator(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  indicator_types:List[Any],
  pattern:String,
  pattern_type:String,
  pattern_version:String,
  valid_from:Date,
  valid_util:Date,
  kill_chain_phases:List[Any])

case class Infrastructure(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  infrastructure_types:List[Any],
  aliases:List[Any],
  kill_chain_phases:List[Any],
  first_seen:Date,
  last_seen:Date)

case class IntrusionSet(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  aliases:List[Any],
  first_seen:Date,
  last_seen:Date,
  goals:List[Any],
  resource_level:String,
  primary_motivation:String,
  secondary_motivations:List[Any])

case class KillChainPhase(
  kill_chain_name:String,
  phase_name:String = null
)

case class Location(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  latitude:Float,
  longitude:Float,
  precision:Float,
  region:String,
  country:String,
  administrative_area:String,
  city:String,
  street_address:String,
  postal_code:String)

case class Malware(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  aliases:List[Any],
  first_seen:Date,
  last_seen:Date,
  malware_types:List[Any],
  is_family:Boolean = false,
  kill_chain_phases:List[Any],
  operating_system_refs:List[Any],
  architecture_execution_envs:List[Any],
  implementation_languages:List[Any],
  capabilities:List[Any],
  sample_refs:List[Any])

case class MalwareAnalysis(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  product:String,
  version:String,
  host_vm_ref:String,
  operating_system_ref:String,
  installed_software_ref:List[Any],
  configuration_version:String,
  modules:List[Any],
  analysis_engine_version:String,
  analysis_definition_version:String,
  submitted:Date,
  analysis_started:Date,
  analysis_ended:Date,
  result_name:String,
  result:String,
  analysis_sco_refs:List[Any],
  sample_ref:String)

case class MarkingDefinition(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  definition_type:String,
  definition:Map[String,Any]
)

case class Note(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  _abstract:String,
  content:String,
  authors:List[Any],
  object_refs:List[Any])

case class ObservedData(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  first_observed:Date,
  last_observed:Date,
  number_observed:Int = 1,
  objects:Map[String,Any],
  object_refs:List[Any])

case class Opinion(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  explanation:String,
  authors:List[Any],
  opinion:String,
  object_refs:List[Any])

case class Report(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  report_types:List[Any],
  published:Date,
  object_refs:List[Any])

case class ThreatActor(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  aliases:List[Any],
  first_seen:Date,
  last_seen:Date,
  threat_actor_types:List[Any],
  roles:List[Any],
  goals:List[Any],
  sophistication:String,
  resource_level:String,
  primary_motivation:String,
  secondary_motivations:List[Any],
  personal_motivations:List[Any])

case class Tool(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String,
  tool_types:List[Any],
  kill_chain_phases:List[Any],
  tool_version:String)

case class Vulnerability(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  labels:List[Any],
  confidence:Int,
  lang:String,
  external_references:List[Any],
  object_marking_refs:List[Any],
  granular_markings:List[Any],
  name:String,
  description:String)

case class Relationship(
  `type`:String,
  spec_version:String="2.1",
  id:String,
  label:String = "relationship",
  created:Date,
  modified:Date,
  created_by_ref:String,
  revoked:Boolean = true,
  external_references:List[Any],
  relationship_type:List[Any],
  source_ref:String,
  target_ref:String)