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

import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.common.MetricsStats;
import com.twitter.heron.healthmgr.common.StatsCollector;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_EXE_COUNT;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_UNSATURATEDCOMP_HIGHCONF;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_UNSATURATEDCOMP_LOWCONF;

public class UnsaturatedComponentDetector implements IDetector {

  private static final Logger LOG = Logger.getLogger(UnsaturatedComponentDetector.class.getName());
  private final ExecuteCountSensor executeCountSensor;
  private StatsCollector statsCollector;

  @Inject
  UnsaturatedComponentDetector(ExecuteCountSensor executeCountSensor,
                               StatsCollector statsCollector) {
    this.executeCountSensor = executeCountSensor;
    this.statsCollector = statsCollector;
  }

  /**
   * Detects all components whose processing rate is lower than what has been observed before
   * (unsaturated components)
   *
   * @return A collection of all unsaturated components.
   */
  @Override
  public List<Symptom> detect() {
    ArrayList<Symptom> result = new ArrayList<>();

    Map<String, ComponentMetrics> processingRates = executeCountSensor.get();
    for (ComponentMetrics compMetrics : processingRates.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      MetricsStats stats = compStats.computeMinMaxStats(METRIC_EXE_COUNT);
      double maxProcessingRateObserved = this.statsCollector.getProcessingRateStats(
          compMetrics.getName()).get();
      boolean backpressureObserved = this.statsCollector.getBackpressureData(compMetrics.getName());
      if (backpressureObserved && stats.getMetricMax() < maxProcessingRateObserved) {
        LOG.info(String.format("Detected unsaturated component with high confidence %s: current "
                + "maximum processing " + "rate is %f, previous observed maximum rate is %f",
            compMetrics.getName(), stats.getMetricMax(), maxProcessingRateObserved));
        result.add(new Symptom(SYMPTOM_UNSATURATEDCOMP_HIGHCONF, compMetrics));
      } else if (!backpressureObserved) {
        if (stats.getMetricMax() <= 0.8 * maxProcessingRateObserved) {
          LOG.info(String.format("Detected unsaturated component with high confidence %s: current "
                  + "maximum processing " + "rate is %f, previous observed maximum rate is %f",
              compMetrics.getName(), stats.getMetricMax(), maxProcessingRateObserved));
          result.add(new Symptom(SYMPTOM_UNSATURATEDCOMP_HIGHCONF, compMetrics));
        } else if (stats.getMetricMax() > 0.8 * maxProcessingRateObserved
            && stats.getMetricMax() <= maxProcessingRateObserved) {
          LOG.info(String.format("Detected unsaturated component with low confidence %s: current "
                  + "maximum processing " + "rate is %f, previous observed maximum rate is %f",
              compMetrics.getName(), stats.getMetricMax(), maxProcessingRateObserved));
          result.add(new Symptom(SYMPTOM_UNSATURATEDCOMP_LOWCONF, compMetrics));
        }
      }
    }
    return result;
  }
}
