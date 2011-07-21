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
import com.cloudera.lib.wsrs.UserProvider;
import org.slf4j.MDC;

/**
 * Class for do-as parameter.
 */
public class DoAsParam extends StringParam {

  /**
   * Parameter name.
   */
  public static final String NAME = "doas";

  /**
   * Default parameter value.
   */
  public static final String DEFAULT = "";

  /**
   * Constructor.
   *
   * @param str parameter value.
   */
  public DoAsParam(String str) {
    super(NAME, str, UserProvider.USER_PATTERN);
  }

  /**
   * Delegates to parent and then adds do-as user to
   * MDC context for logging purposes.
   *
   * @param name parameter name.
   * @param str parameter value.
   * @return parsed parameter
   */
  @Override
  public String parseParam(String name, String str) {
    String doAs = super.parseParam(name, str);
    MDC.put("doAs", (doAs != null) ? doAs : "-");
    return doAs;
  }
}
