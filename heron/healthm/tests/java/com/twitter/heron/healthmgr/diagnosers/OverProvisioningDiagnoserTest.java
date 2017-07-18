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

import java.util.ArrayList;
import java.util.List;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.TestUtils;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_EXE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OverProvisioningDiagnoserTest {
  @Test
  public void diagnosisWhenSmallWaitQueue() {
    List<Symptom> symptoms = new ArrayList<>();
    symptoms.add(TestUtils.createLowConfUnsaturatedComponentSymptom(100));
    symptoms.add(TestUtils.createSmallWaitQSymptom(1));
    Diagnosis result = new OverProvisioningDiagnoser().diagnose(symptoms);
    validateDiagnosisWithSmallQueue(result);
  }

  @Test
  public void diagnosisWhenSmallGrowingWaitQueue() {
    List<Symptom> symptoms = new ArrayList<>();
    symptoms.add(TestUtils.createLowConfUnsaturatedComponentSymptom(100));
    symptoms.add(TestUtils.createSmallWaitQSymptom(1));
    symptoms.add(TestUtils.createGrowingWaitingQueueSymptom(1));
    Diagnosis result = new OverProvisioningDiagnoser().diagnose(symptoms);
    assertNull(result);
  }

  @Test
  public void diagnosisWhenSmallWaitQueueNoUnsatComp() {
    List<Symptom> symptoms = new ArrayList<>();
    symptoms.add(TestUtils.createSmallWaitQSymptom(1));
    Diagnosis result = new OverProvisioningDiagnoser().diagnose(symptoms);
    assertNull(result);
  }

  @Test
  public void diagnosisWhenHighConfUnsaturatedComponent() {
    List<Symptom> symptoms = new ArrayList<>();
    symptoms.add(TestUtils.createHighConfUnsaturatedComponentSymptom(100));
    Diagnosis result = new OverProvisioningDiagnoser().diagnose(symptoms);
    validateDiagnosisWithUnsaturatedComponent(result);
  }

  @Test
  public void diagnosisWhenHighConfUnsaturatedComponentWithGrowingQ() {
    List<Symptom> symptoms = new ArrayList<>();
    symptoms.add(TestUtils.createHighConfUnsaturatedComponentSymptom(100));
    symptoms.add(TestUtils.createGrowingWaitingQueueSymptom(1));
    Diagnosis result = new OverProvisioningDiagnoser().diagnose(symptoms);
    assertNull(result);
  }

  @Test
  public void diagnosisBothSymptoms() {
    List<Symptom> symptoms = new ArrayList<>();
    symptoms.add(TestUtils.createSmallWaitQSymptom(1));
    symptoms.add(TestUtils.createHighConfUnsaturatedComponentSymptom(100));
    Diagnosis result = new OverProvisioningDiagnoser().diagnose(symptoms);
    validateDiagnosisWithUnsaturatedComponent(result);
  }

  @Test
  public void diagnosisFails() {
    List<Symptom> symptoms = new ArrayList<>();
    Diagnosis result = new OverProvisioningDiagnoser().diagnose(symptoms);
    assertNull(result);
  }

  private void validateDiagnosisWithSmallQueue(Diagnosis result) {
    assertEquals(1, result.getSymptoms().size());
    ComponentMetrics data = result.getSymptoms().values().iterator().next().getComponent();
    assertEquals(1,
        data.getMetricValueSum("container_1_bolt_0", METRIC_BUFFER_SIZE).intValue());
  }

  private void validateDiagnosisWithUnsaturatedComponent(Diagnosis result) {
    assertEquals(1, result.getSymptoms().size());
    ComponentMetrics data = result.getSymptoms().values().iterator().next().getComponent();
    assertEquals(100,
        data.getMetricValueSum("container_1_bolt_0", METRIC_EXE_COUNT).intValue());
  }
}
