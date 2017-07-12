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
import java.util.Map;
import java.util.logging.Logger;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.DIAGNOSIS_OVER_PROVISIONING;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_OVER_PROVISIONING;

public class OverProvisioningDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(OverProvisioningDiagnoser.class.getName());

  @Override
  public Diagnosis diagnose(List<Symptom> symptoms) {
    Map<String, ComponentMetrics> unsaturatedComponents =
        getUnsaturatedComponents(symptoms);
    Map<String, ComponentMetrics> smallWaitQComponents = getSmallWaitQComponents(symptoms);
    Symptom resultSymptom = null;

    if (!unsaturatedComponents.isEmpty()) {
      String component = unsaturatedComponents.keySet().iterator().next();
      resultSymptom = new Symptom(SYMPTOM_OVER_PROVISIONING, unsaturatedComponents.get(component));
      LOG.info(String.format("OVER_PROVISIONING: %s is unsaturated", component));
    } else if (!smallWaitQComponents.isEmpty()) {
      String component = smallWaitQComponents.keySet().iterator().next();
      resultSymptom = new Symptom(SYMPTOM_OVER_PROVISIONING, smallWaitQComponents.get(component));
      LOG.info(String.format("OVER_PROVISIONING: %s has a small queue size", component));
    }
    return resultSymptom != null ? new Diagnosis(DIAGNOSIS_OVER_PROVISIONING, resultSymptom) : null;
  }
}
