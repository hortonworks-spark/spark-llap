/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.hive.utils

import java.net.{URL, URLClassLoader}
import java.util

import org.apache.spark.internal.Logging

object HiveIsolatedClassLoader extends Logging {

  def isolatedClassLoader(): ClassLoader = {
    val parentClassLoader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)

    // Assume the task parent classloader is either Spark MutableURLClassLoader or
    // ExecutorClassLoader
    def getAddedURLs(classloader: ClassLoader): Array[URL] = {
      classloader match {
        case e if e.getClass.getName == "org.apache.spark.repl.ExecutorClassLoader" =>
          val method = e.getClass.getMethod("parentLoader")
          method.setAccessible(true)
          val parent = method.invoke(e).asInstanceOf[ClassLoader].getParent
          getAddedURLs(parent)

        case e if e.getClass.getName == "org.apache.spark.util.ChildFirstURLClassLoader" =>
          val method = e.getClass.getMethod("parentClassLoader")
          method.setAccessible(true)
          val parent = method.invoke(e).asInstanceOf[ClassLoader].getParent
          getAddedURLs(parent)

        case e if e.getClass.getName == "org.apache.spark.util.MutableURLClassLoader" =>
          val method = e.getClass.getMethod("getURLs")
          method.setAccessible(true)
          method.invoke(e).asInstanceOf[Array[URL]]

        case e: ClassLoader =>
          Option(e.getParent).map { getAddedURLs(_) }.getOrElse(
            throw new IllegalStateException("Get unexpected classloader"))

        case u =>
          throw new IllegalStateException(s"Get unexpected object, $u")
      }
    }

    val urls = getAddedURLs(parentClassLoader)
    new HiveIsolatedClassLoader(urls, parentClassLoader)
  }
}

class HiveIsolatedClassLoader(urls: Array[URL], baseClassLoader: ClassLoader)
  extends URLClassLoader(urls, ClassLoader.getSystemClassLoader.getParent.getParent)
    with Logging {

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    val loaded = findLoadedClass(name)
    if (loaded == null) doLoadClass(name, resolve) else loaded
  }

  override def getResource(name: String): URL = {
    baseClassLoader.getResource(name)
  }

  override def getResources(name: String): util.Enumeration[URL] = {
    baseClassLoader.getResources(name)
  }

  def doLoadClass(name: String, resolve: Boolean): Class[_] = {
    if (isHiveClass(name)) {
      logTrace(s"hive class: $name - ${super.getResource(classToPath(name))}")
      super.loadClass(name, resolve)
    } else {
      try {
        baseClassLoader.loadClass(name)
      } catch {
        case _: ClassNotFoundException =>
          super.loadClass(name, resolve)
      }
    }
  }

  private def isHiveClass(name: String): Boolean = {
    name.startsWith("org.apache.hadoop.hive.") ||
      name.startsWith("org.apache.hive.") ||
      name.startsWith("org.apache.orc.")
  }

  private def classToPath(name: String): String =
    name.replaceAll("\\.", "/") + ".class"
}

