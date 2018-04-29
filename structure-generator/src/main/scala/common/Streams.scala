package common

import com.google.common.io.Closer
import com.google.common.io.Files
import org.apache.commons.io.IOUtils
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.util.Properties

object Streams {
  /**
    * Returns a Properties class for the given input stream.
    */
  def readProperties(input: InputStream): Properties = {
    assert(input != null, "InputStream can't be null")
    try {
      val props = new Properties
      props.load(input)
      props
    } catch {
      case e: IOException => throw e
    }
  }

  /**
    * Method to load properties file using the classloader from the current thread.
    */
  def readProperties(name: String): Properties = {
    assert(name != null, "String name can't be null")
    val resourceAsStream = Thread.currentThread.getContextClassLoader.getResourceAsStream(name)
    try {
      val props = new Properties
      props.load(resourceAsStream)
      props
    } catch {
      case e: IOException => throw e
    }
  }

  /**
    * Method to load a resource file using the classloader from the current thread.
    */
  def readResource(name: String): InputStream = {
    assert(name != null, "String name can't be null")
    Thread.currentThread.getContextClassLoader.getResourceAsStream(name)
  }

  def copyResourceToFile(resource: String, file: File): Unit = {
    val closer = Closer.create
    try {
      val is = Streams.readResource(resource)
      assert(is != null, "Resource '" + resource + "' does not exist")
      closer.register(is)
      Files.createParentDirs(file)
      val os = closer.register(new FileOutputStream(file))
      IOUtils.copy(is, os)
    } catch {
      case e: IOException => throw e
    } finally try
      closer.close()
    catch {
      case e: IOException => throw e
    }
  }
}
