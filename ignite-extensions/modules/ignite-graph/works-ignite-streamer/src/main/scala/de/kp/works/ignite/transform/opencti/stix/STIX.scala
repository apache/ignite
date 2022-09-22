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

object STIX {

  val ABSTRACT_BASIC_OBJECT = "basic-object"
  val ABSTRACT_STIX_OBJECT = "stix-object"
  val ABSTRACT_STIX_META_OBJECT = "stix-meta-object"
  val ABSTRACT_STIX_CORE_OBJECT = "stix-core-object"
  val ABSTRACT_STIX_DOMAIN_OBJECT = "stix-domain-object"
  val ABSTRACT_STIX_CYBER_OBSERVABLE = "stix-cyber-observable"
  val ABSTRACT_STIX_CYBER_OBSERVABLE_HASHED_OBSERVABLE = "hashed-observable"
  val ABSTRACT_INTERNAL_OBJECT = "internal-object"

  val ENTITY_TYPE_ATTACK_PATTERN = "attack-pattern"
  val ENTITY_TYPE_CAMPAIGN = "campaign"
  val ENTITY_TYPE_CONTAINER_NOTE = "note"
  val ENTITY_TYPE_CONTAINER_OBSERVED_DATA = "observed-data"
  val ENTITY_TYPE_CONTAINER_OPINION = "opinion"
  val ENTITY_TYPE_CONTAINER_REPORT = "report"
  val ENTITY_TYPE_COURSE_OF_ACTION = "course-of-action"
  val ENTITY_TYPE_IDENTITY = "identity"
  val ENTITY_TYPE_INCIDENT = "incident"
  val ENTITY_TYPE_INDICATOR = "indicator"
  val ENTITY_TYPE_INFRASTRUCTURE = "infrastructure"
  val ENTITY_TYPE_INTRUSION_SET = "intrusion-Set"
  val ENTITY_TYPE_LOCATION = "location"
  val ENTITY_TYPE_MALWARE = "malware"
  val ENTITY_TYPE_THREAT_ACTOR = "threat-actor"
  val ENTITY_TYPE_TOOL = "tool"
  val ENTITY_TYPE_VULNERABILITY = "vulnerability"

  val ENTITY_TYPE_CONTAINER = "container"

  val ENTITY_TYPE_LABEL = "label"
  val ENTITY_TYPE_EXTERNAL_REFERENCE = "external-reference"
  val ENTITY_TYPE_KILL_CHAIN_PHASE = "kill-chain-phase"

  /*
   * The list of OpenCTI STIX Cyber Observable types
   */
  val ENTITY_AUTONOMOUS_SYSTEM = "autonomous-system"
  val ENTITY_DIRECTORY = "directory"
  val ENTITY_DOMAIN_NAME = "domain-name"
  val ENTITY_EMAIL_ADDR = "email-addr"
  val ENTITY_EMAIL_MESSAGE = "email-message"
  val ENTITY_EMAIL_MIME_PART_TYPE = "email-mime-part-type"
  val ENTITY_HASHED_OBSERVABLE_ARTIFACT = "artifact"
  val ENTITY_HASHED_OBSERVABLE_FILE = "file"
  val ENTITY_HASHED_OBSERVABLE_X509_CERTIFICATE = "x509-certificate"
  val ENTITY_IPV4_ADDR = "ipv4-addr"
  val ENTITY_IPV6_ADDR = "ipv6-addr"
  val ENTITY_MAC_ADDR = "mac-addr"
  val ENTITY_MUTEX = "mutex"
  val ENTITY_NETWORK_TRAFFIC = "network-traffic"
  val ENTITY_PROCESS = "process"
  val ENTITY_SOFTWARE = "software"
  val ENTITY_URL = "url"
  val ENTITY_USER_ACCOUNT = "user-account"
  val ENTITY_WINDOWS_REGISTRY_KEY = "windows-registry-key"
  val ENTITY_WINDOWS_REGISTRY_VALUE_TYPE = "windows-registry-value-type"
  val ENTITY_X509_V3_EXTENSIONS_TYPE = "x509-v3-extensions-type"
  val ENTITY_X_OPENCTI_CRYPTOGRAPHIC_KEY = "x-opencti-cryptographic-key"
  val ENTITY_X_OPENCTI_CRYPTOGRAPHIC_WALLET = "x-opencti-cryptocurrency-wallet"
  val ENTITY_X_OPENCTI_HOSTNAME = "x-opencti-hostname"
  val ENTITY_X_OPENCTI_TEXT = "x-opencti-text"
  val ENTITY_X_OPENCTI_USER_AGENT = "x-opencti-user-agent"

  val ENTITY_TYPE_SETTINGS = "Settings"
  val ENTITY_TYPE_MIGRATION_STATUS = "MigrationStatus"
  val ENTITY_TYPE_MIGRATION_REFERENCE = "MigrationReference"
  val ENTITY_TYPE_RULE_MANAGER = "RuleManager"
  val ENTITY_TYPE_GROUP = "Group"
  val ENTITY_TYPE_USER = "User"
  val ENTITY_TYPE_RULE = "Rule"
  val ENTITY_TYPE_ROLE = "Role"
  val ENTITY_TYPE_CAPABILITY = "Capability"
  val ENTITY_TYPE_CONNECTOR = "Connector"
  val ENTITY_TYPE_ATTRIBUTE = "Attribute"
  val ENTITY_TYPE_WORKSPACE = "Workspace"
  val ENTITY_TYPE_WORK = "work"
  val ENTITY_TYPE_TASK = "Task"
  val ENTITY_TYPE_TAXII_COLLECTION = "TaxiiCollection"
  val ENTITY_TYPE_STREAM_COLLECTION = "StreamCollection"
  val ENTITY_TYPE_USER_SUBSCRIPTION = "UserSubscription"

  val STIX_DOMAIN_OBJECTS: Array[String] = Array(
    ENTITY_TYPE_ATTACK_PATTERN,
    ENTITY_TYPE_CAMPAIGN,
    ENTITY_TYPE_CONTAINER_NOTE,
    ENTITY_TYPE_CONTAINER_OBSERVED_DATA,
    ENTITY_TYPE_CONTAINER_OPINION,
    ENTITY_TYPE_CONTAINER_REPORT,
    ENTITY_TYPE_COURSE_OF_ACTION,
    ENTITY_TYPE_IDENTITY,
    ENTITY_TYPE_INDICATOR,
    ENTITY_TYPE_INFRASTRUCTURE,
    ENTITY_TYPE_INTRUSION_SET,
    ENTITY_TYPE_LOCATION,
    ENTITY_TYPE_MALWARE,
    ENTITY_TYPE_THREAT_ACTOR,
    ENTITY_TYPE_TOOL,
    ENTITY_TYPE_VULNERABILITY,
    ENTITY_TYPE_INCIDENT
  )

  val STIX_DOMAIN_OBJECT_CONTAINERS: Array[String] = Array(
    ENTITY_TYPE_CONTAINER_NOTE,
    ENTITY_TYPE_CONTAINER_OBSERVED_DATA,
    ENTITY_TYPE_CONTAINER_OPINION,
    ENTITY_TYPE_CONTAINER_REPORT
  )
  val INTERNAL_OBJECTS: Array[String] = Array(
    ENTITY_TYPE_SETTINGS,
    ENTITY_TYPE_TAXII_COLLECTION,
    ENTITY_TYPE_STREAM_COLLECTION,
    ENTITY_TYPE_USER_SUBSCRIPTION,
    ENTITY_TYPE_TASK,
    ENTITY_TYPE_MIGRATION_STATUS,
    ENTITY_TYPE_MIGRATION_REFERENCE,
    ENTITY_TYPE_GROUP,
    ENTITY_TYPE_USER,
    ENTITY_TYPE_ROLE,
    ENTITY_TYPE_RULE,
    ENTITY_TYPE_RULE_MANAGER,
    ENTITY_TYPE_CAPABILITY,
    ENTITY_TYPE_CONNECTOR,
    ENTITY_TYPE_ATTRIBUTE,
    ENTITY_TYPE_WORKSPACE
  )

  val STIX_META_OBJECT: Array[String] = Array(
    ENTITY_TYPE_LABEL,
    ENTITY_TYPE_EXTERNAL_REFERENCE,
    ENTITY_TYPE_KILL_CHAIN_PHASE
  )

  val STIX_CYBER_OBSERVABLES_HASHED_OBSERVABLES: Array[String] = Array(
    ENTITY_HASHED_OBSERVABLE_ARTIFACT,
    ENTITY_HASHED_OBSERVABLE_FILE,
    ENTITY_HASHED_OBSERVABLE_X509_CERTIFICATE
  )

  val STIX_CYBER_OBSERVABLES: Array[String] = Array(
    ENTITY_AUTONOMOUS_SYSTEM,
    ENTITY_DIRECTORY,
    ENTITY_DOMAIN_NAME,
    ENTITY_EMAIL_ADDR,
    ENTITY_EMAIL_MESSAGE,
    ENTITY_EMAIL_MIME_PART_TYPE,
    ENTITY_HASHED_OBSERVABLE_ARTIFACT,
    ENTITY_HASHED_OBSERVABLE_FILE,
    ENTITY_HASHED_OBSERVABLE_X509_CERTIFICATE,
    ENTITY_X509_V3_EXTENSIONS_TYPE,
    ENTITY_IPV4_ADDR,
    ENTITY_IPV6_ADDR,
    ENTITY_MAC_ADDR,
    ENTITY_MUTEX,
    ENTITY_NETWORK_TRAFFIC,
    ENTITY_PROCESS,
    ENTITY_SOFTWARE,
    ENTITY_URL,
    ENTITY_USER_ACCOUNT,
    ENTITY_WINDOWS_REGISTRY_KEY,
    ENTITY_WINDOWS_REGISTRY_VALUE_TYPE,
    ENTITY_X_OPENCTI_CRYPTOGRAPHIC_KEY,
    ENTITY_X_OPENCTI_CRYPTOGRAPHIC_WALLET,
    ENTITY_X_OPENCTI_HOSTNAME,
    ENTITY_X_OPENCTI_USER_AGENT,
    ENTITY_X_OPENCTI_TEXT
  )

  val STIX_INDICATORS: Array[String] = Array(

  )

  /** HASHES * */
  val MD5 = "MD5"
  val SHA_1 = "SHA-1"
  val SHA_256 = "SHA-256"
  val SHA_512 = "SHA-512"
  val SHA3_256 = "SHA3-256"
  val SHA3_512 = "SHA3-512"
  val SSDEEP = "SSDEEP"

  val STANDARD_HASHES: Array[String] = Array(
    MD5,
    SHA_1,
    SHA_256,
    SHA_512,
    SHA3_256,
    SHA3_512,
    SSDEEP)

  def isStixDomainObjectContainer(`type`: String): Boolean = {
    STIX_DOMAIN_OBJECT_CONTAINERS.contains(`type`) || `type` == ENTITY_TYPE_CONTAINER
  }

  def isStixDomainObject(`type`: String): Boolean = {
    STIX_DOMAIN_OBJECTS.contains(`type`) ||
      isStixDomainObjectContainer(`type`) ||
      `type` == ABSTRACT_STIX_DOMAIN_OBJECT
  }

  def isStixHashedObservable(entityType: String): Boolean = {
    STIX_CYBER_OBSERVABLES_HASHED_OBSERVABLES.contains(entityType)
  }

  def isCyberObservable(entityType: String): Boolean = {
    STIX_CYBER_OBSERVABLES.contains(entityType) || isStixHashedObservable(entityType)
  }

  def isStixCoreObject(`type`: String): Boolean = {
    isStixDomainObject(`type`) || isCyberObservable(`type`) || `type` == ABSTRACT_STIX_CORE_OBJECT
  }

  def isStixMetaObject(`type`: String): Boolean = {
    STIX_META_OBJECT.contains(`type`) || `type` == ABSTRACT_STIX_META_OBJECT
  }

  def isStixObject(`type`: String): Boolean = {
    isStixCoreObject(`type`) || isStixMetaObject(`type`) || `type` == ABSTRACT_STIX_OBJECT
  }

  def isInternalObject(`type`: String): Boolean = {
    INTERNAL_OBJECTS.contains(`type`) || `type` == ABSTRACT_INTERNAL_OBJECT
  }

  def isBasicObject(`type`: String): Boolean = {
    isInternalObject(`type`) || isStixObject(`type`) || `type` == ABSTRACT_BASIC_OBJECT
  }

  /**
   * RELATIONSHIP SUPPORT
   */
  val RELATION_CREATED_BY = "created-by"
  val RELATION_EXTERNAL_REFERENCE = "external-reference"
  val RELATION_KILL_CHAIN_PHASE = "kill-chain-phase"
  val RELATION_OBJECT = "object"
  val RELATION_OBJECT_MARKING = "object-marking"
  val RELATION_OBJECT_LABEL = "object-label"

  val STIX_CORE_RELATIONSHIP = "relationship"
  val STIX_SIGHTING_RELATIONSHIP = "sighting"

  val RELATION_OPERATING_SYSTEM = "operating-system"
  val RELATION_SAMPLE = "sample"
  val RELATION_CONTAINS = "contains"
  val RELATION_RESOLVES_TO = "resolves-to"
  val RELATION_BELONGS_TO = "obs_belongs-to"
  val RELATION_FROM = "from"
  val RELATION_SENDER = "sender"
  val RELATION_TO = "to"
  val RELATION_CC = "cc"
  val RELATION_BCC = "bcc"
  val RELATION_RAW_EMAIL = "raw-email"
  val RELATION_BODY_RAW = "body-raw"
  val RELATION_PARENT_DIRECTORY = "parent-directory"
  val RELATION_RELATION_CONTENT = "relation-content"
  val RELATION_SRC = "src"
  val RELATION_DST = "dst"
  val RELATION_SRC_PAYLOAD = "src-payload"
  val RELATION_DST_PAYLOAD = "dst-payload"
  val RELATION_ENCAPSULATES = "encapsulates"
  val RELATION_ENCAPSULATED_BY = "encapsulated-by"
  val RELATION_OPENED_CONNECTION = "opened-connection"
  val RELATION_CREATOR_USER = "creator-user"
  val RELATION_IMAGE = "image"
  val RELATION_PARENT = "parent"
  val RELATION_CHILD = "child"
  val RELATION_BODY_MULTIPART = "body-multipart"
  val RELATION_VALUES = "values"
  val RELATION_X509_V3_EXTENSIONS = "x509-v3-extensions"
  val RELATION_LINKED = "x_opencti_linked-to"

  val STIX_EXTERNAL_META_RELATIONSHIPS: Array[String] = Array(
    RELATION_CREATED_BY,
    RELATION_OBJECT_MARKING,
    RELATION_OBJECT)

  val STIX_INTERNAL_META_RELATIONSHIPS: Array[String] = Array(
    RELATION_OBJECT_LABEL,
    RELATION_EXTERNAL_REFERENCE,
    RELATION_KILL_CHAIN_PHASE
  )

  val STIX_META_RELATIONSHIPS: Array[String] = STIX_EXTERNAL_META_RELATIONSHIPS ++ STIX_INTERNAL_META_RELATIONSHIPS

  val STIX_OBSERVABLE_RELATIONSHIPS: Array[String] = Array(
    RELATION_OPERATING_SYSTEM,
    RELATION_SAMPLE,
    RELATION_CONTAINS,
    RELATION_RESOLVES_TO,
    RELATION_BELONGS_TO,
    RELATION_FROM,
    RELATION_SENDER,
    RELATION_TO,
    RELATION_CC,
    RELATION_BCC,
    RELATION_RAW_EMAIL,
    RELATION_BODY_RAW,
    RELATION_PARENT_DIRECTORY,
    RELATION_RELATION_CONTENT,
    RELATION_SRC,
    RELATION_DST,
    RELATION_SRC_PAYLOAD,
    RELATION_DST_PAYLOAD,
    RELATION_ENCAPSULATES,
    RELATION_ENCAPSULATED_BY,
    RELATION_OPENED_CONNECTION,
    RELATION_CREATOR_USER,
    RELATION_IMAGE,
    RELATION_PARENT,
    RELATION_CHILD,
    RELATION_BODY_MULTIPART,
    RELATION_VALUES,
    RELATION_X509_V3_EXTENSIONS,
    RELATION_LINKED
  )

  def isStixRelationship(`type`: String): Boolean = {
    `type` == STIX_CORE_RELATIONSHIP
  }

  def isStixSighting(`type`: String): Boolean = {
    `type` == STIX_SIGHTING_RELATIONSHIP
  }

  def isStixInternalMetaRelationship(`type`: String): Boolean = {
    STIX_INTERNAL_META_RELATIONSHIPS.contains(`type`)
  }

  def isStixMetaRelationship(`type`: String): Boolean = {
    STIX_META_RELATIONSHIPS.contains(`type`)
  }

  def isStixObservableRelationship(`type`: String): Boolean = {
    STIX_OBSERVABLE_RELATIONSHIPS.contains(`type`)
  }

  def isStixEdge(`type`: String): Boolean = {
    isStixRelationship(`type`) ||
    isStixSighting(`type`) ||
    isStixMetaRelationship(`type`) ||
    isStixObservableRelationship(`type`)
  }
}
