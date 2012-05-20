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

import com.cloudera.lib.wsrs.EnumParam;

/**
 * Class for GET operation parameter.
 */
public class GetOpParam extends EnumParam<GetOpParam.Values> {

  /**
   * Get operations.
   */
  public enum Values {
    OPEN, STATUS, LIST, HOMEDIR, INSTRUMENTATION, GETFILESTATUS, LISTSTATUS
  }

  /**
   * Parameter name.
   */
  public static final String NAME = "op";

  /**
   * Default parameter value.
   */
  public static final String DEFAULT = "open";

  /**
   * Constructor.
   *
   * @param str parameter value.
   */
  public GetOpParam(String str) {
    super(NAME, str, Values.class);
  }
}
