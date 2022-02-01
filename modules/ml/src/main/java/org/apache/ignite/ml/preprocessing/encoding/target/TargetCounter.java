/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.preprocessing.encoding.target;

import java.util.HashMap;
import java.util.Map;

/**
 * Counter for encode category.
 */
public class TargetCounter {
  /** */
  private Double targetSum = 0d;

  /** */
  private Long targetCount = 0L;

  /** */
  private final Map<String, Long> categoryCounts = new HashMap<>();

  /** */
  private final Map<String, Double> categoryTargetSum = new HashMap<>();

  /** */
  public Double getTargetSum() {
    return targetSum;
  }

  /** */
  public void setTargetSum(Double targetSum) {
    this.targetSum = targetSum;
  }

  /** */
  public Long getTargetCount() {
    return targetCount;
  }

  /** */
  public void setTargetCount(Long targetCount) {
    this.targetCount = targetCount;
  }

  /** */
  public Map<String, Long> getCategoryCounts() {
    return categoryCounts;
  }

  /** */
  public void setCategoryCounts(Map<String, Long> categoryCounts) {
    this.categoryCounts.putAll(categoryCounts);
  }

  /** */
  public Map<String, Double> getCategoryTargetSum() {
    return categoryTargetSum;
  }

  /** */
  public void setCategoryTargetSum(Map<String, Double> categoryTargetSum) {
    this.categoryTargetSum.putAll(categoryTargetSum);
  }
}
