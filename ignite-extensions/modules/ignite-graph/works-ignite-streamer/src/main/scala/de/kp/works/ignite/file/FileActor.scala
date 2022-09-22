package de.kp.works.ignite.file
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.Source
import com.google.gson.JsonParser
import de.kp.works.ignite.conf.WorksConf

import java.io.FileNotFoundException
import java.nio.file.Path
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

object FileActor {

  sealed trait FileEvent

  case class Created() extends FileEvent
  case class Deleted() extends FileEvent
  case class Modified() extends FileEvent

}

class FileActor(
  name:String,
  path:Path,
  eventHandler:FileEventHandler) extends Actor with ActorLogging {

  import FileActor._
  /**
   * The actor system is implicitly accompanied by a materializer,
   * and this materializer is required to retrieve the Bytestring
   */
  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  /**
   * The [FileTailSource] starts at a given offset in a file and emits chunks of bytes until
   * reaching the end of the file, it will then poll the file for changes and emit new changes
   * as they are written to the file (unless there is backpressure).
   *
   * A very common use case is combining reading bytes with parsing the bytes into lines, therefore
   * FileTailSource contains a few factory methods to create a source that parses the bytes into
   * lines and emits those.
   */
  private var fileSource: Option[Source[String, NotUsed]] = None

  private val receiverCfg = WorksConf.getReceiverCfg(name)
  private val maxLineSize = receiverCfg.getInt("maxLineSize")
  /*
   * The polling interval for file changes
   * is predefined
   */
  private val pollingInterval = 250.millis

  override def receive: Receive = {
    case _:Created =>
      /*
       * Initialize the file source and stream lines
       * to event handler
       */
      val fileTailSource = FileTailSource.lines(
        path = path,
        maxLineSize = maxLineSize,
        pollingInterval = pollingInterval
      )
        .recoverWithRetries(1, {
          case _: FileNotFoundException => Source.empty
        })

      fileSource = Some(fileTailSource)
      fileSource.get.runForeach(line => send(line))

    case _:Deleted =>
      /*
       * Do nothing as the file source is configured
       * (see `recoverWithRetries`) to automatically
       * shutdown, if the respective fill is deleted
       */
      context.stop(self)

    case _:Modified =>
    /*
     * Do nothing as the file source is implemented
     * to automatically detected changes
     */
    case _ =>
      throw new Exception(s"Unknown file event detected")
  }

  def send(line:String):Unit = {
    try {
      /*
       * Check whether the provided line is
       * a JSON line
       */
      val json = JsonParser.parseString(line)

      val event = FileEvent(eventType = path.toFile.getName, eventData = json.toString)
      eventHandler.eventArrived(event)

    } catch {
      case _:Throwable => /* Do nothing */
    }
  }

}


