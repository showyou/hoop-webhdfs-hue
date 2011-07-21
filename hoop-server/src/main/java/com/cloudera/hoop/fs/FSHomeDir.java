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

import com.cloudera.hoop.HoopServer;
import com.cloudera.lib.service.Hadoop;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;

import java.io.IOException;

/**
 * Executor that performs a home-dir Hadoop files system operation.
 */
public class FSHomeDir implements Hadoop.FileSystemExecutor<JSONObject> {

  /**
   * Executes the filesystem operation.
   *
   * @param fs filesystem instance to use.
   * @return a JSON object with the user home directory.
   * @throws IOException thrown if an IO error occured.
   */
  @Override
  @SuppressWarnings("unchecked")
  public JSONObject execute(FileSystem fs) throws IOException {
    Path homeDir = fs.getHomeDirectory();
    JSONObject json = new JSONObject();
    homeDir = FSUtils.convertPathToHoop(homeDir, HoopServer.get().getBaseUrl());
    json.put("homeDir", homeDir.toString());
    return json;
  }

}
