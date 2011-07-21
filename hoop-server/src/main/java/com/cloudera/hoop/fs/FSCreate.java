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
import com.cloudera.lib.io.IOUtils;
import com.cloudera.lib.service.Hadoop;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Executor that performs a create Hadoop files system operation.
 */
public class FSCreate implements Hadoop.FileSystemExecutor<URI> {
  private InputStream is;
  private Path path;
  private String permission;
  private boolean override;
  private short replication;
  private long blockSize;

  /**
   * Creates a Create executor.
   *
   * @param is input stream to for the file to create.
   * @param path path of the file to create.
   * @param perm permission for the file.
   * @param override if the file should be overriden if it already exist.
   * @param repl the replication factor for the file.
   * @param blockSize the block size for the file.
   */
  public FSCreate(InputStream is, String path, String perm, boolean override, short repl, long blockSize) {
    this.is = is;
    this.path = new Path(path);
    this.permission = perm;
    this.override = override;
    this.replication = repl;
    this.blockSize = blockSize;
  }

  /**
   * Executes the filesystem operation.
   *
   * @param fs filesystem instance to use.
   * @return The URI of the created file.
   * @throws IOException thrown if an IO error occured.
   */
  @Override
  public URI execute(FileSystem fs) throws IOException {
    if (replication == -1) {
      replication = (short) fs.getConf().getInt("dfs.replication", 3);
    }
    if (blockSize == -1) {
      blockSize = fs.getConf().getInt("dfs.block.size", 67108864);
    }
    FsPermission fsPermission = FSUtils.getPermission(permission);
    int bufferSize = fs.getConf().getInt("hoop.buffer.size", 4096);
    OutputStream os = fs.create(path, fsPermission, override, bufferSize, replication, blockSize, null);
    IOUtils.copy(is, os);
    os.close();
    return FSUtils.convertPathToHoop(path, HoopServer.get().getBaseUrl()).toUri();
  }

}
