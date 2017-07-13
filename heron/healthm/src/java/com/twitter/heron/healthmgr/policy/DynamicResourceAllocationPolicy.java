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


package com.twitter.heron.healthmgr.policy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.inject.Inject;

import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.HealthPolicyImpl;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.detectors.LargeWaitQueueDetector;
import com.twitter.heron.healthmgr.detectors.ProcessingRateSkewDetector;
import com.twitter.heron.healthmgr.detectors.SmallWaitQueueDetector;
import com.twitter.heron.healthmgr.detectors.UnsaturatedComponentDetector;
import com.twitter.heron.healthmgr.detectors.WaitQueueDisparityDetector;
import com.twitter.heron.healthmgr.diagnosers.DataSkewDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.OverProvisioningDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.SlowInstanceDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.UnderProvisioningDiagnoser;
import com.twitter.heron.healthmgr.resolvers.ScaleDownResolver;
import com.twitter.heron.healthmgr.resolvers.ScaleUpResolver;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.HEALTH_POLICY_INTERVAL;

public class DynamicResourceAllocationPolicy extends HealthPolicyImpl
    implements EventHandler<TopologyUpdate> {

  public static final String CONF_WAIT_INTERVAL_MILLIS =
      "DynamicResourceAllocationPolicy.conf_post_action_wait_interval_min";
  private static final Logger LOG
      = Logger.getLogger(DynamicResourceAllocationPolicy.class.getName());
  private HealthPolicyConfig policyConfig;
  private ScaleUpResolver scaleUpResolver;
  private ScaleDownResolver scaleDownResolver;


  @Inject
  DynamicResourceAllocationPolicy(HealthPolicyConfig policyConfig,
                                  EventManager eventManager,
                                  BackPressureDetector backPressureDetector,
                                  SmallWaitQueueDetector smallWaitQueueDetector,
                                  UnsaturatedComponentDetector unsaturatedComponentDetector,
                                  ProcessingRateSkewDetector dataSkewDetector,
                                  WaitQueueDisparityDetector waitQueueDisparityDetector,
                                  UnderProvisioningDiagnoser underProvisioningDiagnoser,
                                  OverProvisioningDiagnoser overProvisioningDiagnoser,
                                  DataSkewDiagnoser dataSkewDiagnoser,
                                  SlowInstanceDiagnoser slowInstanceDiagnoser,
                                  ScaleUpResolver scaleUpResolver,
                                  ScaleDownResolver scaleDownResolver) {
    this.policyConfig = policyConfig;
    this.scaleUpResolver = scaleUpResolver;
    this.scaleDownResolver = scaleDownResolver;

    registerDetectors(backPressureDetector, smallWaitQueueDetector, unsaturatedComponentDetector,
        waitQueueDisparityDetector, dataSkewDetector);
    registerDiagnosers(underProvisioningDiagnoser, overProvisioningDiagnoser,
        dataSkewDiagnoser, slowInstanceDiagnoser);

    setPolicyExecutionInterval(TimeUnit.MILLISECONDS,
        Long.valueOf(policyConfig.getConfig(HEALTH_POLICY_INTERVAL, "60000")));

    eventManager.addEventListener(TopologyUpdate.class, this);
  }

  @Override
  public IResolver selectResolver(List<Diagnosis> diagnosis) {
    Map<String, Diagnosis> diagnosisMap = new HashMap<>();
    for (Diagnosis diagnoses : diagnosis) {
      diagnosisMap.put(diagnoses.getName(), diagnoses);
    }

    if (diagnosisMap.containsKey(DataSkewDiagnoser.class.getName())) {
      LOG.warning("Data Skew diagnoses. This diagnosis does not have any resolver.");
    } else if (diagnosisMap.containsKey(SlowInstanceDiagnoser.class.getName())) {
      LOG.warning("Slow Instance diagnoses. This diagnosis does not have any resolver.");
    } else if (diagnosisMap.containsKey(UnderProvisioningDiagnoser.class.getSimpleName())) {
      return scaleUpResolver;
    } else if (diagnosisMap.containsKey(OverProvisioningDiagnoser.class.getSimpleName())){
      return scaleDownResolver;
    }

    return null;
  }

  @Override
  public void onEvent(TopologyUpdate event) {
    int interval = Integer.valueOf(policyConfig.getConfig(CONF_WAIT_INTERVAL_MILLIS, "180000"));
    LOG.info("Received topology update action event: " + event);
    setOneTimeDelay(TimeUnit.MILLISECONDS, interval);
  }
}
