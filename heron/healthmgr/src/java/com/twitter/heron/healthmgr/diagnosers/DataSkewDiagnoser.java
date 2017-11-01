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


package com.twitter.heron.healthmgr.diagnosers;

import java.util.List;
import java.util.logging.Logger;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.common.MetricsStats;

import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_DATA_SKEW;
import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.SYMPTOM_DATA_SKEW;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;

public class DataSkewDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(DataSkewDiagnoser.class.getName());

  @Override
  public Diagnosis diagnose(List<Symptom> symptoms) {
    List<Symptom> bpSymptoms = getBackPressureSymptoms(symptoms);
    ComponentMetrics processingRateSkewMetrics = getProcessingRateSkewComponents(symptoms);
    ComponentMetrics waitQDisparityMetrics = getWaitQDisparityComponents(symptoms);

    if (bpSymptoms.isEmpty() || processingRateSkewMetrics.isEmpty()
        || waitQDisparityMetrics.isEmpty()) {
      // Since there is no back pressure or disparate execute count, no action is needed
      return null;
    } else if (bpSymptoms.size() > 1) {
      // TODO handle cases where multiple detectors create back pressure symptom
      throw new IllegalStateException("Multiple back-pressure symptoms case");
    }

    ComponentMetrics bpMetrics = bpSymptoms.iterator().next().getComponentMetrics();
    if (bpMetrics.getComponentCount() != 1) {
      // TODO handle cases where multiple detectors create back pressure symptom
      throw new IllegalStateException("Multiple back-pressure symptoms case");
    }
    String compCausingBp = bpMetrics.getComponentNames().iterator().next();

    // verify data skew, larger queue size and back pressure for the same component exists
    ComponentMetrics exeCountMetrics = processingRateSkewMetrics.filterByComponent(compCausingBp);
    ComponentMetrics pendingBufferMetrics = waitQDisparityMetrics.filterByComponent(compCausingBp);
    if (exeCountMetrics.isEmpty() || pendingBufferMetrics.isEmpty()) {
      // no processing rate skew and buffer size skew
      // for the component with back pressure. This is not a data skew case
      return null;
    }

    ComponentMetrics mergedData = ComponentMetrics.merge(bpMetrics,
        ComponentMetrics.merge(exeCountMetrics, pendingBufferMetrics));
    ComponentMetricsHelper compStats = new ComponentMetricsHelper(mergedData);
    compStats.computeBpStats();
    MetricsStats exeStats = compStats.computeMinMaxStats(METRIC_EXE_COUNT);
    MetricsStats bufferStats = compStats.computeMinMaxStats(METRIC_BUFFER_SIZE);

    Symptom resultSymptom = null;
    for (InstanceMetrics boltMetrics : compStats.getBoltsWithBackpressure()) {
      String compName = boltMetrics.getComponentName();
      String instName = boltMetrics.getInstanceName();

      if (exeCountMetrics.filterByInstance(compName, instName).isEmpty()) {
        continue;
      }
      double exeCount = exeCountMetrics.filterByInstance(compName, instName)
          .getMetrics().iterator().next().getValueSum();

      if (pendingBufferMetrics.filterByInstance(compName, instName).isEmpty()) {
        continue;
      }
      double bufferSize = pendingBufferMetrics.filterByInstance(compName, instName)
          .getMetrics().iterator().next().getValueSum();

      double bpValue = boltMetrics.getValueSum();

      if (exeStats.getMetricMax() < 1.10 * exeCount
          && bufferStats.getMetricMax() < 2 * bufferSize) {
        LOG.info(String.format("DataSkew: %s back-pressure(%s), high execution count: %s and "
            + "high buffer size %s", instName, bpValue, exeCount, bufferSize));
        resultSymptom = new Symptom(SYMPTOM_DATA_SKEW.text(), mergedData);
      }
    }

    return resultSymptom != null ? new Diagnosis(DIAGNOSIS_DATA_SKEW.text(), resultSymptom) : null;
  }
}
