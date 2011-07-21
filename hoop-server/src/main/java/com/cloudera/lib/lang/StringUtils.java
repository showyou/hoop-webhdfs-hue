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
package com.cloudera.lib.lang;

import java.util.Collection;

/**
 * String related utilities.
 */
public abstract class StringUtils {

  /**
   * Flattens a collection elements to a string calling
   * <code>toString()</code> on each element.
   *
   * @param collection collection elements to flatten.
   * @param separator separator to use between elements.
   * @return the flattened string representation of the collection.
   */
  public static String toString(Collection<?> collection, String separator) {
    String toString = "L[null]";
    if (collection != null) {
      StringBuilder sb = new StringBuilder(collection.size() * 20);
      String sep = "";
      for (Object obj : collection) {
        sb.append(sep).append(obj);
        sep = separator;
      }
      toString = sb.toString();
    }
    return toString;
  }

}
