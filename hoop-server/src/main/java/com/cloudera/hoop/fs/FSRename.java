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
import org.json.simple.JSONObject;

import java.io.IOException;

/**
 * Executor that performs a rename Hadoop files system operation.
 */
public class FSRename implements Hadoop.FileSystemExecutor<JSONObject> {
  private Path path;
  private Path toPath;

  /**
   * Creates a rename executor.
   *
   * @param path path to rename.
   * @param toPath new name.
   */
  public FSRename(String path, String toPath) {
    this.path = new Path(path);
    this.toPath = new Path(toPath);
  }

  /**
   * Executes the filesystem operation.
   *
   * @param fs filesystem instance to use.
   * @return <code>true</code> if the rename operation was successful,
   * <code>false</code> otherwise.
   * @throws IOException thrown if an IO error occured.
   */
  @Override
  public JSONObject execute(FileSystem fs) throws IOException {
    boolean renamed = fs.rename(path, toPath);
    return FSUtils.toJSON("rename", renamed);
  }

}
