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
package com.cloudera.hoop;

import com.cloudera.lib.wsrs.StringParam;

/**
 * Class for path parameter.
 */
public class FsPathParam extends StringParam {

  /**
   * Constructor.
   *
   * @param path parameter value.
   */
  public FsPathParam(String path) {
    super("path", path);
  }

  /**
   * Makes the path absolute adding '/' to it.
   * <p/>
   * This is required because JAX-RS resolution of paths does not add
   * the root '/'.
   * 
   * @returns absolute path.
   */
  public void makeAbsolute() {
    String path = value();
    path = "/" + ((path != null) ? path : "");
    setValue(path);
  }

}
