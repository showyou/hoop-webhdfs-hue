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

import com.cloudera.hoop.fs.FSUtils;
import com.cloudera.lib.wsrs.StringParam;

import java.util.regex.Pattern;

/**
 * Class for permission parameter.
 */
public class PermissionParam extends StringParam {

  /**
   * Parameter name.
   */
  public static final String NAME = "permission";

  /**
   * Default parameter value.
   */
  public static final String DEFAULT = FSUtils.DEFAULT_PERMISSION;


  /**
   * Symbolic Unix permissions regular expression pattern.
   */
  private static final Pattern PERMISSION_PATTERN =
    Pattern.compile(DEFAULT + "|(-[-r][-w][-x][-r][-w][-x][-r][-w][-x])");

  /**
   * Constructor.
   *
   * @param permission parameter value.
   */
  public PermissionParam(String permission) {
    super(NAME, permission.toLowerCase(), PERMISSION_PATTERN);
  }

}
