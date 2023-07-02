package de.kp.works.ignite.streamer.fiware
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
import scala.collection.mutable

object FiwareSubscriptions {

  /**
   * SAMPLE SUBSCRIPTION
   */
  private val sample = """
    subscription {
      #
      # The description to specify this subscription
      #
      description = ""
      subject = {
        entities = [
          {id = "", type = ""},
        ]
        #
        # The condition element defines the "trigger" for the subscription. The "attrs" field
        # contains a list of attribute names. These names define the "triggering attributes",
        # i.e. attributes that upon creation/change due to entity creation or update trigger
        # the notification.
        #
        # The rule is that if at least one of the attributes in the list changes (e.g. some
        # kind of "OR" condition), then a notification is sent. An empty attribute vector
        # specifies that a notification is triggered any entity attribute change (regardless
        # of the name of the attribute).
        #
        condition = {
          attrs = ["temperature",]
        }
      }
      notification = {
        http = {
          #
          # The url where to send notifications. Only one url can be included
          # per subscription. However, you can have several subscriptions on
          # the same context elements (i.e. same entity and attribute).
          #
          url = "http://localhost:9080/v2/notify"
        }
        #
        #
        # The "attrs" field defines a list of attribute names that will be included
        # in the notification. An empty attribute vector indicates that all attributes
        # of the specified entities will be published.
        #
        attrs = ["temperature",]
      }
      #
      # Subscriptions may have an expiration date (expires field), specified using
      # the ISO 8601 standard format. Once a subscription overpass that date, the
      # subscription is simply ignored.
      #
      # You can also have permanent subscriptions. Just omit the expires field.
      #
      expires = "2040-01-01T14:00:00.00Z"
      #
      # The throttling element is used to specify a minimum inter-notification arrival time.
      # So, setting throttling to 5 seconds as in the example below, makes a notification not
      # to be sent if a previous notification was sent less than 5 seconds earlier, no matter
      # how many actual changes take place in that period.
      #
      # This is to give the notification receptor a means to protect itself against context
      # producers that update attribute values too frequently.
      #
      throttling: 5
    }
  """

  private val registry:mutable.HashMap[String, String] = mutable.HashMap.empty[String,String]

  def getSubscriptions:Seq[String] = {
    Seq.empty[String]
  }

  def register(sid:String, subscription:String):Unit = {
    registry += sid -> subscription
  }

  def isRegistered(sid:String):Boolean = {
    registry.contains(sid)
  }
}
