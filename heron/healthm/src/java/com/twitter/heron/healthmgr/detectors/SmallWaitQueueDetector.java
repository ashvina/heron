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


package com.twitter.heron.healthmgr.detectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.inject.Inject;

import com.microsoft.dhalion.api.IDetector;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.common.MetricsStats;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_SMALL_WAIT_Q;

public class SmallWaitQueueDetector implements IDetector {
  public static final String SMALL_WAIT_QUEUE_SIZE_LIMIT = "SmallWaitQueueDetector.threshold";

  private static final Logger LOG = Logger.getLogger(SmallWaitQueueDetector.class.getName());
  private final BufferSizeSensor pendingBufferSensor;
  private final double threshold;

  @Inject
  SmallWaitQueueDetector(BufferSizeSensor pendingBufferSensor,
                         HealthPolicyConfig policyConfig) {
    this.pendingBufferSensor = pendingBufferSensor;
    threshold = Double.valueOf(policyConfig.getConfig(SMALL_WAIT_QUEUE_SIZE_LIMIT, "5"));
  }

  /**
   * Detects all components that have small wait queues
   *
   * @return A collection of all components with small wait queues.
   */
  @Override
  public List<Symptom> detect() {
    ArrayList<Symptom> result = new ArrayList<>();

    Map<String, ComponentMetrics> bufferSizes = pendingBufferSensor.get();
    for (ComponentMetrics compMetrics : bufferSizes.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      MetricsStats stats = compStats.computeMinMaxStats(METRIC_BUFFER_SIZE);
      if (stats.getMetricMax() <= threshold) {
        LOG.info(String.format("Detected small wait queues for %s, largest queue is %f",
            compMetrics.getName(), stats.getMetricMax()));
        result.add(new Symptom(SYMPTOM_SMALL_WAIT_Q, compMetrics));
      }
    }
    return result;
  }
}
