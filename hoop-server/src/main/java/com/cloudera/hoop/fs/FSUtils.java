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
package com.cloudera.hoop.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * File system utilities used by Hoop classes.
 */
public class FSUtils {

  /**
   * Constant for the default permission string ('default').
   */
  public static final String DEFAULT_PERMISSION = "default";
  
  /**
   * FSFile System's constant value
   */
  public static final String FILE_STATUSES_JSON = "FileStatuses";
  public static final String FILE_STATUS_JSON = "FileStatus";
  public static final String PATH_SUFFIX_JSON = "pathSuffix";
  public static final String TYPE_JSON = "type";
  public static final String LENGTH_JSON = "length";
  public static final String OWNER_JSON = "owner";
  public static final String GROUP_JSON = "group";
  public static final String PERMISSION_JSON = "permission";
  public static final String ACCESS_TIME_JSON = "accessTime";
  public static final String MODIFICATION_JSON = "modificationTime";
  public static final String BLOCK_SIZE_JSON_JSON = "blockSize";
  public static final String REPLICATION_JSON = "replication";

  public static enum FILE_TYPE {
    FILE, DIRECTORY, SYMLINK;

    public static FILE_TYPE getType(FileStatus fileStatus) {
        if (fileStatus.isDir()) {
            return DIRECTORY;
        }
        return FILE;
        //
        // throw IllegalArgumentException("Could not detemine filetype for: "+
        //                              fileStatus.getPath());
    }
  }
  /**
   * Converts a Unix permission symbolic representation
   * (i.e. -rwxr--r--) into a Hadoop permission.
   *
   * @param str Unix permission symbolic representation.
   * @return the Hadoop permission. If the given string was
   * 'default', it returns <code>FsPermission.getDefault()</code>.
   */
  public static FsPermission getPermission(String str) {
    FsPermission permission;
    if (str.equals(DEFAULT_PERMISSION)) {
      permission = FsPermission.getDefault();
    }
    else {
      //TODO: there is something funky here, it does not detect 'x'
      permission = FsPermission.valueOf(str);
    }
    return permission;
  }

  /**
   * Replaces the <code>SCHEME://HOST:PORT</code> of a Hadoop
   * <code>Path</code> with the specified base URL.
   *
   * @param path Hadoop path to replace the
   * <code>SCHEME://HOST:PORT</code>.
   * @param hoopBaseUrl base URL to replace it with.
   * @return the path using the given base URL.
   */
  public static Path convertPathToHoop(Path path, String hoopBaseUrl) {
    URI uri = path.toUri();
    String filePath = uri.getRawPath();
    String hoopPath = hoopBaseUrl + filePath;
    return new Path(hoopPath);
  }

  /**
   * Converts a Hadoop permission into a Unix permission symbolic
   * representation (i.e. -rwxr--r--) or default if the permission is NULL.
   *
   * @param p Hadoop permission.
   * @return the Unix permission symbolic representation or default if the
   * permission is NULL.
   */
  private static String permissionToString(FsPermission p) {
    return (p == null) ? "default" : "-" + p.getUserAction().SYMBOL + p.getGroupAction().SYMBOL +
                                     p.getOtherAction().SYMBOL;
  }

  /**
   * Converts a Hadoop <code>FileStatus</code> object into a JSON array
   * object. It replaces the <code>SCHEME://HOST:PORT</code> of the path
   * with the  specified URL.
   * <p/>
   * @param status Hadoop file status.
   * @param hoopBaseUrl base URL to replace the
   * <code>SCHEME://HOST:PORT</code> in the file status.
   * @return The JSON representation of the file status.
   */
  @SuppressWarnings("unchecked")
  public static Map fileStatusToJSONRaw(FileStatus status, String hoopBaseUrl) {
    Map json = new LinkedHashMap();
    json.put(PATH_SUFFIX_JSON, convertPathToHoop(status.getPath(), hoopBaseUrl).toString());
    json.put(TYPE_JSON, FILE_TYPE.getType(status).toString());
    json.put(LENGTH_JSON, status.getLen());
    json.put(OWNER_JSON, status.getOwner());
    json.put(GROUP_JSON, status.getGroup());
    json.put(PERMISSION_JSON, permissionToString(status.getPermission()));
    json.put(ACCESS_TIME_JSON, status.getAccessTime());
    json.put(MODIFICATION_JSON, status.getModificationTime());
    json.put(BLOCK_SIZE_JSON_JSON, status.getBlockSize());
    json.put(REPLICATION_JSON, status.getReplication());
    return json;
  }
  

  @SuppressWarnings("unchecked")
  public static Map fileStatusToJSON(FileStatus status, String hoopBaseUrl) {
    Map response = new LinkedHashMap();
    response.put(FILE_STATUS_JSON, fileStatusToJSONRaw(status, hoopBaseUrl));

    return response;
  }


  /**
   * Converts a Hadoop <code>FileStatus</code> array into a JSON array
   * object. It replaces the <code>SCHEME://HOST:PORT</code> of the path
   * with the  specified URL.
   * <p/>
   * @param status Hadoop file status array.
   * @param hoopBaseUrl base URL to replace the
   * <code>SCHEME://HOST:PORT</code> in the file status.
   * @return The JSON representation of the file status array.
   */
  @SuppressWarnings("unchecked")
  public static Map fileStatusToJSON(FileStatus[] status, String hoopBaseUrl) {
    JSONArray json = new JSONArray();
    if (status != null) {
      for (FileStatus s : status) {
        json.add(fileStatusToJSON(s, hoopBaseUrl));
      }
    }
    Map temp = new LinkedHashMap();
    Map response = new LinkedHashMap();
    temp.put(FILE_STATUS_JSON, json);
    response.put(FILE_STATUSES_JSON, temp);

    return response;
  }

  /**
   * Converts an object into a Json Map with with one key-value entry.
   * <p/>
   * It assumes the given value is either a JSON primitive type or a
   * <code>JsonAware</code> instance.
   *
   * @param name name for the key of the entry.
   * @param value for the value of the entry.
   * @return the JSON representation of the key-value pair.
   */
  @SuppressWarnings("unchecked")
  public static JSONObject toJSON(String name, Object value) {
    JSONObject json = new JSONObject();
    json.put(name, value);
    return json;
  }
}
