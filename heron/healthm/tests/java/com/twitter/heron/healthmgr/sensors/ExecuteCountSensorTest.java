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

package com.twitter.heron.healthmgr.sensors;

import java.util.HashMap;
import java.util.Map;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetricsData;
import com.microsoft.dhalion.metrics.InstanceMetricsData;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.HealthManagerContstants;
import com.twitter.heron.healthmgr.common.TopologyProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecuteCountSensorTest {
  @Test
  public void providesBoltExecutionCountMetrics() {
    TopologyProvider topologyProvider = mock(TopologyProvider.class);
    when(topologyProvider.getBoltNames()).thenReturn(new String[]{"bolt-1", "bolt-2"});

    MetricsProvider metricsProvider = mock(MetricsProvider.class);

    Map<String, ComponentMetricsData> result = new HashMap<>();

    ComponentMetricsData metrics = new ComponentMetricsData("bolt-1");
    metrics.addInstanceMetric(createTestInstanceMetric("container_1_bolt-1_1", 123));
    metrics.addInstanceMetric(createTestInstanceMetric("container_1_bolt-1_2", 345));
    result.put("bolt-1", metrics);

    metrics = new ComponentMetricsData("bolt-2");
    metrics.addInstanceMetric(createTestInstanceMetric("container_1_bolt-2_3", 321));
    metrics.addInstanceMetric(createTestInstanceMetric("container_1_bolt-2_4", 543));
    result.put("bolt-2", metrics);

    when(metricsProvider
        .getComponentMetrics(HealthManagerContstants.METRIC_EXE_COUNT, 60, "bolt-1", "bolt-2"))
        .thenReturn(result);

    ExecuteCountSensor executeCountSensor = new ExecuteCountSensor(topologyProvider, metricsProvider);
    Map<String, ComponentMetricsData> componentMetrics = executeCountSensor.get();
    assertEquals(2, componentMetrics.size());
    assertEquals(123, componentMetrics.get("bolt-1")
        .getMetrics("container_1_bolt-1_1")
        .getMetricIntValue(HealthManagerContstants.METRIC_EXE_COUNT));
    assertEquals(345, componentMetrics.get("bolt-1")
        .getMetrics("container_1_bolt-1_2")
        .getMetricIntValue(HealthManagerContstants.METRIC_EXE_COUNT));
    assertEquals(321, componentMetrics.get("bolt-2")
        .getMetrics("container_1_bolt-2_3")
        .getMetricIntValue(HealthManagerContstants.METRIC_EXE_COUNT));
    assertEquals(543, componentMetrics.get("bolt-2")
        .getMetrics("container_1_bolt-2_4")
        .getMetricIntValue(HealthManagerContstants.METRIC_EXE_COUNT));
  }

  private InstanceMetricsData createTestInstanceMetric(String name, int value) {
    InstanceMetricsData instanceMetrics = new InstanceMetricsData(name);
    instanceMetrics.addMetric(HealthManagerContstants.METRIC_EXE_COUNT, value);
    return instanceMetrics;
  }
}
