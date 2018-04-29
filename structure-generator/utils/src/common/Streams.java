package common;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.google.common.io.Files;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public final class Streams {

  private Streams() {}

  /**
   * Returns a Properties class for the given input stream.
   * 
   * <p>Use {@link #readProperties(String)} if you want to use current ClassLoader
   */
  public static Properties readProperties(InputStream input) {
    checkNotNull(input, "InputStream can't be null");
    
    try {
      Properties props = new Properties();
      props.load(input);
      return props;
    } catch (IOException e) { 
     throw Throwables.propagate(e);
    }
  }

  /**
   * Method to load properties file using the classloader from the current
   * thread.
   * 
   * @param name The file name on the classpath
   * @return The properties file
   */
  public static Properties readProperties(String name) {
    checkNotNull(name, "String name can't be null");

    InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    try {
      Properties props = new Properties();
      props.load(resourceAsStream);
      return props;
    } catch (IOException e) { 
      throw Throwables.propagate(e);
    }
  }

  /**
   * Method to load a resource file using the classloader from the current
   * thread.
   * 
   * @param name The file name on the classpath
   * @return The stream
   */
  public static InputStream readResource(String name) {
    checkNotNull(name, "String name can't be null");

    return Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
  }

  public static void copyResourceToFile(String resource, File file) {
    Closer closer = Closer.create();
    try {
      InputStream is = Streams.readResource(resource);
      checkNotNull(is, "Resource '" + resource + "' does not exist");
      closer.register(is);

      Files.createParentDirs(file);
      OutputStream os = closer.register(new FileOutputStream(file));
      IOUtils.copy(is, os);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}