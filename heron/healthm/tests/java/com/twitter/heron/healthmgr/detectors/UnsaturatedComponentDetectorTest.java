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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Optional;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.StatsCollector;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.google.common.base.Optional.*;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_EXE_COUNT;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_UNSATURATEDCOMP_HIGHCONF;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_UNSATURATEDCOMP_LOWCONF;
import static com.twitter.heron.healthmgr.detectors.SmallWaitQueueDetector.SMALL_WAIT_QUEUE_SIZE_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class UnsaturatedComponentDetectorTest {

  @Test
  public void testHighConfUnsaturatedComponentwithBackpressure() {
    StatsCollector statsCollector =  new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double)1000)).when(spyCollector).getProcessingRateStats("bolt");
    doReturn(true).when(spyCollector).getBackpressureData("bolt");

    ComponentMetrics compMetrics = new ComponentMetrics("bolt", "i1", METRIC_EXE_COUNT, 5);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    ExecuteCountSensor sensor = mock(ExecuteCountSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    UnsaturatedComponentDetector detector = new UnsaturatedComponentDetector(sensor, spyCollector);
    List<Symptom> symptoms = detector.detect();
    assertEquals(SYMPTOM_UNSATURATEDCOMP_HIGHCONF, symptoms.get(0).getName());
    assertEquals(1, symptoms.size());
  }

  @Test
  public void testHighConfUnsaturatedComponentNoBackpressure() {
    StatsCollector statsCollector =  new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double)1000)).when(spyCollector).getProcessingRateStats("bolt");
    doReturn(false).when(spyCollector).getBackpressureData("bolt");

    ComponentMetrics compMetrics = new ComponentMetrics("bolt", "i1", METRIC_EXE_COUNT, 500);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    ExecuteCountSensor sensor = mock(ExecuteCountSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    UnsaturatedComponentDetector detector = new UnsaturatedComponentDetector(sensor, spyCollector);
    List<Symptom> symptoms = detector.detect();
    assertEquals(SYMPTOM_UNSATURATEDCOMP_HIGHCONF, symptoms.get(0).getName());
    assertEquals(1, symptoms.size());
  }

  @Test
  public void testLowConfUnsaturatedComponent() {
    StatsCollector statsCollector =  new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double)1000)).when(spyCollector).getProcessingRateStats("bolt");
    doReturn(false).when(spyCollector).getBackpressureData("bolt");

    ComponentMetrics compMetrics = new ComponentMetrics("bolt", "i1", METRIC_EXE_COUNT, 900);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    ExecuteCountSensor sensor = mock(ExecuteCountSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    UnsaturatedComponentDetector detector = new UnsaturatedComponentDetector(sensor, spyCollector);
    List<Symptom> symptoms = detector.detect();
    assertEquals(SYMPTOM_UNSATURATEDCOMP_LOWCONF, symptoms.get(0).getName());
    assertEquals(1, symptoms.size());
  }

  @Test
  public void testNegativeUnsaturatedComponent() {
    StatsCollector statsCollector =  new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double)1000)).when(spyCollector).getProcessingRateStats("bolt");

    ComponentMetrics compMetrics = new ComponentMetrics("bolt", "i1", METRIC_EXE_COUNT, 1001);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    ExecuteCountSensor sensor = mock(ExecuteCountSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    UnsaturatedComponentDetector detector = new UnsaturatedComponentDetector(sensor, spyCollector);
    List<Symptom> symptoms = detector.detect();

    assertEquals(0, symptoms.size());
  }
}
