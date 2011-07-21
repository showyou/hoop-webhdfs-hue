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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

/**
 * Executor that performs a file-status Hadoop files system operation.
 */
public class FSFileStatus implements Hadoop.FileSystemExecutor<Map> {
  private Path path;

  /**
   * Creates a file-status executor.
   *
   * @param path the path to retrieve the status.
   */
  public FSFileStatus(String path) {
    this.path = new Path(path);
  }

  /**
   * Executes the filesystem operation.
   *
   * @param fs filesystem instance to use.
   * @return a Map object (JSON friendly) with the file status.
   * @throws IOException thrown if an IO error occured.
   */
  @Override
  public Map execute(FileSystem fs) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    return FSUtils.fileStatusToJSON(status, HoopServer.get().getBaseUrl());
  }

}
