// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.healthmgr.common;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;

public class StatsCollector {

  public Map<String, Double> processingRateStats = new HashMap<>();
  public List<String> backpressureData = new ArrayList<>();

  public Optional<Double> getProcessingRateStats(String componentName) {
    if (processingRateStats.containsKey(componentName)) {
      return Optional.of(processingRateStats.get(componentName));
    }
    return Optional.absent();
  }

  public Boolean getBackpressureData(String componentName) {
    if (backpressureData.contains(componentName)) {
      return true;
    }
    return false;
  }
}
