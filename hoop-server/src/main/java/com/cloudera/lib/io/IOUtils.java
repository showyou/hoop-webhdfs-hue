/*
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.lib.io;

import com.cloudera.lib.util.Check;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public abstract class IOUtils {

  /**
   * Deletes the specified path recursively.
   * <p/>
   * As a safety mechanism, it converts the specified file to an absolute
   * path and it throws an exception if the path is shorter than 5 character
   * (you dont want to delete things like /tmp, /usr, /etc, /var, /opt, /sbin, /dev).
   *
   * @param file path to delete.
   * @throws IOException thrown if an IO error occurred.
   */
  public static void delete(File file) throws IOException {
    if (file.getAbsolutePath().length() < 5) {
      throw new IllegalArgumentException(
        MessageFormat.format("Path [{0}] is too short, not deleting", file.getAbsolutePath()));
    }
    if (file.exists()) {
      if (file.isDirectory()) {
        File[] children = file.listFiles();
        if (children != null) {
          for (File child : children) {
            delete(child);
          }
        }
      }
      if (!file.delete()) {
        throw new RuntimeException(MessageFormat.format("Could not delete path [{0}]", file.getAbsolutePath()));
      }
    }
  }

  /**
   * Copies an inputstream to an outputstream.
   *
   * @param is inputstream to copy.
   * @param os target outputstream.
   * @throws IOException thrown if an IO error occurred.
   */
  public static void copy(InputStream is, OutputStream os) throws IOException {
    Check.notNull(is, "is");
    Check.notNull(os, "os");
    copy(is, os, 0, -1);
  }

  /**
   * Copies a range of bytes from an inputstream to an outputstream.
   *
   * @param is inputstream to copy.
   * @param os target outputstream.
   * @param offset of the inputstream to start the copy from, the offset
   * is relative to the current position.
   * @param len length of the inputstream to copy, <code>-1</code> means
   * until the end of the inputstream.
   * @throws IOException thrown if an IO error occurred.
   */
  public static void copy(InputStream is, OutputStream os, long offset, long len) throws IOException {
    Check.notNull(is, "is");
    Check.notNull(os, "os");
    byte[] buffer = new byte[1024];
    long skipped = is.skip(offset);
    if (skipped == offset) {
      if (len == -1) {
        int read = is.read(buffer);
        while (read > -1) {
          os.write(buffer, 0, read);
          read = is.read(buffer);
        }
        is.close();
      }
      else {
        long count = 0;
        int read = is.read(buffer);
        while (read > -1 && count < len) {
          count += read;
          if (count < len) {
            os.write(buffer, 0, read);
            read = is.read(buffer);
          }
          else if (count == len) {
            os.write(buffer, 0, read);
          }
          else {
            int leftToWrite = read - (int) (count - len);
            os.write(buffer, 0, leftToWrite);
          }
        }
      }
      os.flush();
    }
    else {
      throw new IOException(MessageFormat.format("InputStream ended before offset [{0}]", offset));
    }
  }

  /**
   * Copies a reader to a writer.
   *
   * @param reader reader to copy.
   * @param writer target writer.
   * @throws IOException thrown if an IO error occurred.
   */
  public static void copy(Reader reader, Writer writer) throws IOException {
    Check.notNull(reader, "reader");
    Check.notNull(writer, "writer");
    copy(reader, writer, 0, -1);
  }

  /**
   * Copies a range of chars from a reader to a writer.
   *
   * @param reader reader to copy.
   * @param writer target writer.
   * @param offset of the reader to start the copy from, the offset
   * is relative to the current position.
   * @param len length of the reader to copy, <code>-1</code> means
   * until the end of the reader.
   * @throws IOException thrown if an IO error occurred.
   */
  public static void copy(Reader reader, Writer writer, long offset, long len) throws IOException {
    Check.notNull(reader, "reader");
    Check.notNull(writer, "writer");
    Check.ge0(offset, "offset");
    char[] buffer = new char[1024];
    long skipped = reader.skip(offset);
    if (skipped == offset) {
      if (len == -1) {
        int read = reader.read(buffer);
        while (read > -1) {
          writer.write(buffer, 0, read);
          read = reader.read(buffer);
        }
        reader.close();
      }
      else {
        long count = 0;
        int read = reader.read(buffer);
        while (read > -1 && count < len) {
          count += read;
          if (count < len) {
            writer.write(buffer, 0, read);
            read = reader.read(buffer);
          }
          else if (count == len) {
            writer.write(buffer, 0, read);
          }
          else {
            int leftToWrite = read - (int) (count - len);
            writer.write(buffer, 0, leftToWrite);
          }
        }
      }
      writer.flush();
    }
    else {
      throw new IOException(MessageFormat.format("Reader ended before offset [{0}]", offset));
    }
  }

  /**
   * Reads a reader into a string.
   * <p/>
   *
   * @param reader reader to read.
   * @return string with the contents of the reader.
   * @throws IOException thrown if an IO error occurred.
   */
  public static String toString(Reader reader) throws IOException {
    Check.notNull(reader, "reader");
    StringWriter writer = new StringWriter();
    copy(reader, writer);
    return writer.toString();
  }


  /**
   * Zips the contents of a directory into a zip outputstream.
   *
   * @param dir directory contents to zip.
   * @param relativePath relative path top prepend to all files in the zip.
   * @param zos zip output stream
   * @throws IOException thrown if an IO error occurred.
   */
  public static void zipDir(File dir, String relativePath, ZipOutputStream zos) throws IOException {
    Check.notNull(dir, "dir");
    Check.notNull(relativePath, "relativePath");
    Check.notNull(zos, "zos");
    zipDir(dir, relativePath, zos, true);
    zos.close();
  }

  /**
   * This recursive method is used by the {@link #zipDir(File, String, ZipOutputStream)} method.
   * <p/>
   * A special handling is required for the start of the zip (via the start parameter).
   * 
   * @param dir directory contents to zip.
   * @param relativePath relative path top prepend to all files in the zip.
   * @param zos zip output stream
   * @param start indicates if this invocation is the start of the zip.
   * @throws IOException thrown if an IO error occurred.
   */
  private static void zipDir(File dir, String relativePath, ZipOutputStream zos, boolean start) throws IOException {
    String[] dirList = dir.list();
    for (String aDirList : dirList) {
      File f = new File(dir, aDirList);
      if (!f.isHidden()) {
        if (f.isDirectory()) {
          if (!start) {
            ZipEntry dirEntry = new ZipEntry(relativePath + f.getName() + "/");
            zos.putNextEntry(dirEntry);
            zos.closeEntry();
          }
          String filePath = f.getPath();
          File file = new File(filePath);
          zipDir(file, relativePath + f.getName() + "/", zos, false);
        }
        else {
          ZipEntry anEntry = new ZipEntry(relativePath + f.getName());
          zos.putNextEntry(anEntry);
          InputStream is = new FileInputStream(f);
          byte[] arr = new byte[4096];
          int read = is.read(arr);
          while (read > -1) {
            zos.write(arr, 0, read);
            read = is.read(arr);
          }
          is.close();
          zos.closeEntry();
        }
      }
    }
  }

}
