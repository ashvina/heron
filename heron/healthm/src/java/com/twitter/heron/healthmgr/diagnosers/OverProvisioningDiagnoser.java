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
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_OVER_PROVISIONING_SMALLWAITQ;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_OVER_PROVISIONING_UNSATCOMP;

public class OverProvisioningDiagnoser extends BaseDiagnoser {
  private static final Logger LOG = Logger.getLogger(OverProvisioningDiagnoser.class.getName());

  @Override
  public Diagnosis diagnose(List<Symptom> symptoms) {
    Map<String, ComponentMetrics> highConfUnsaturatedComponents =
        getHighConfUnsaturatedComponents(symptoms);
    Map<String, ComponentMetrics> lowConfUnsaturatedComponents =
        getLowConfUnsaturatedComponents(symptoms);
    Map<String, ComponentMetrics> smallWaitQComponents = getSmallWaitQComponents(symptoms);
    Map<String, ComponentMetrics> growingWaitQueueComponents =
        getGrowingWaitQueueComponents(symptoms);
    Symptom resultSymptom = null;

    if (!highConfUnsaturatedComponents.isEmpty()) {
      for (String component : highConfUnsaturatedComponents.keySet()) {
        if (!growingWaitQueueComponents.containsKey(component)) {
          resultSymptom = new Symptom(SYMPTOM_OVER_PROVISIONING_UNSATCOMP,
              highConfUnsaturatedComponents.get(component));
          LOG.info(String.format("OVER_PROVISIONING: %s is unsaturated", component));
          continue;
        }
      }
    } else if (!lowConfUnsaturatedComponents.isEmpty()) {
      for (String component : lowConfUnsaturatedComponents.keySet()) {
        if (!growingWaitQueueComponents.containsKey(component) && smallWaitQComponents
            .containsKey(component)) {
          resultSymptom = new Symptom(SYMPTOM_OVER_PROVISIONING_SMALLWAITQ,
              smallWaitQComponents.get(component));
          LOG.info(String.format("OVER_PROVISIONING: %s has a small queue size", component));
          continue;
        }
      }
    }
    return resultSymptom != null ? new Diagnosis(DIAGNOSIS_OVER_PROVISIONING, resultSymptom) : null;
  }

}
