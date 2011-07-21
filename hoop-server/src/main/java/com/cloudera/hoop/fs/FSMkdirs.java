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

import com.cloudera.lib.service.Hadoop;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.json.simple.JSONObject;

import java.io.IOException;

/**
 * Executor that performs a mkdirs Hadoop files system operation.
 */
public class FSMkdirs implements Hadoop.FileSystemExecutor<JSONObject> {

  private Path path;
  private String permission;

  /**
   * Creates a mkdirs executor.
   *
   * @param path directory path to create.
   * @param permission permission to use.
   */
  public FSMkdirs(String path, String permission) {
    this.path = new Path(path);
    this.permission = permission;
  }

  /**
   * Executes the filesystem operation.
   *
   * @param fs filesystem instance to use.
   * @return <code>true</code> if the mkdirs operation was successful,
   * <code>false</code> otherwise.
   * @throws IOException thrown if an IO error occured.
   */
  @Override
  public JSONObject execute(FileSystem fs) throws IOException {
    FsPermission fsPermission = FSUtils.getPermission(permission);
    boolean mkdirs = fs.mkdirs(path, fsPermission);
    return FSUtils.toJSON("mkdirs", mkdirs);
  }

}
