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

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.util.Timeout
import com.typesafe.config.Config
import de.kp.works.ignite.conf.WorksConf

import java.nio.file.{FileSystem, FileSystems, Path}
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class FileMonitor(name:String, folder:String, eventHandler: FileEventHandler) {

  import FileActor._
  /**
   * Akka 2.6 provides a default materializer out of the box, i.e., for Scala
   * an implicit materializer is provided if there is an implicit ActorSystem
   * available. This avoids leaking materializers and simplifies most stream
   * use cases somewhat.
   */
  private val systemName = WorksConf.getSystemName(name)
  implicit val system: ActorSystem = ActorSystem(systemName)

  implicit lazy val context: ExecutionContextExecutor = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  /**
   * Common timeout for all Akka connection
   */
  implicit val timeout: Timeout = Timeout(5.seconds)

  private val fs: FileSystem = FileSystems.getDefault

  private val receiverCfg: Config = WorksConf.getReceiverCfg(name)

  private val pollingInterval: FiniteDuration =
    FiniteDuration(receiverCfg.getInt("pollingInterval"), "seconds")

  private val maxBufferSize: Int = receiverCfg.getInt("maxBufferSize")
  private val postfix: String = receiverCfg.getString("postfix")
  /*
   * The current implementation assigns an Akka actor
   * to each file
   */
  private val actors = mutable.HashMap.empty[String, ActorRef]

  def start(): Unit = {
    /*
     * Starting the filesystem monitor invokes the [DirectoryChangesSource]
     * to listen to changes in the provided folder. This source will emit elements
     * every time there is a change to a watched directory in the local filesystem,
     */
    val changes = DirectoryChangesSource(
      fs.getPath(folder), pollInterval = pollingInterval, maxBufferSize = maxBufferSize)
    /*
     * This is the entry point for the log file monitor
     *
     * We expect that the log directory does not contain
     * subdirectories; otherwise a recursive directory change
     * source must be used, e.g. from
     *
     * https://github.com/sysco-middleware/alpakka-connectors
    */
    changes.runForeach {
      case (path, change) =>
        change match {
          case DirectoryChange.Creation =>
            onCreated(path)
          case DirectoryChange.Deletion =>
            onDeleted(path)
          case DirectoryChange.Modification =>
            onModified(path)
          case _ => /* Do nothing */
        }
    }

  }

  private def onCreated(path:Path):Unit = {
    if (!isLogFile(path)) return
    /*
     * We expect that the path is not registered
     * already, and that a new File actor must be
     * created.
     */
    val fileName = path.toFile.getName
    if (actors.contains(fileName)) return

    actors += fileName -> buildFileActor(path)
    actors(fileName) ! Created

  }

  private def onDeleted(path:Path):Unit = {
    if (!isLogFile(path)) return

    val fileName = path.toFile.getName
    if (!actors.contains(fileName)) return

    actors(fileName) ! Deleted
    actors.remove(fileName)

  }

  private def onModified(path:Path):Unit = {
    if (!isLogFile(path)) return

    val fileName = path.toFile.getName
    if (!actors.contains(fileName)) return

    actors(fileName) ! Modified

  }
  /**
   * This helper method checks whether the provided
   * path that refers to file change
   */
  private def isLogFile(path:Path):Boolean = {
    /*
     * The current implementation does not support
     * sub directories within the Fleet log folder
     */
    if (!path.toFile.isDirectory) return false

    val fileName = path.toFile.getName
    fileName.endsWith(postfix)

  }
  /**
   * A helper method to build a file listener actor
   */
  /**
   * A helper method to build a file listener actor
   */
  protected def buildFileActor(path:Path):ActorRef = {
    val actorName = "File-Actor-" + java.util.UUID.randomUUID.toString
    system.actorOf(Props(new FileActor(name, path, eventHandler)), actorName)
  }

}
