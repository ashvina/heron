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

import java.util.Optional;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.common.TopologyProvider;

import static com.twitter.heron.healthmgr.sensors.BaseSensor.DEFAULT_METRIC_DURATION;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecuteCountSensorTest {
  @Test
  public void providesBoltExecutionCountMetrics() {
    String metric = METRIC_EXE_COUNT.text();
    TopologyProvider topologyProvider = mock(TopologyProvider.class);
    when(topologyProvider.getBoltNames()).thenReturn(new String[]{"bolt-1", "bolt-2"});

    MetricsProvider metricsProvider = mock(MetricsProvider.class);

    ComponentMetrics metrics = new ComponentMetrics();
    metrics.addMetric("bolt-1", "container_1_bolt-1_1", metric, 123);
    metrics.addMetric("bolt-1", "container_1_bolt-1_2", metric, 345);
    metrics.addMetric("bolt-2", "container_1_bolt-2_3", metric, 321);
    metrics.addMetric("bolt-2", "container_1_bolt-2_4", metric, 543);

    when(metricsProvider
        .getComponentMetrics(metric, DEFAULT_METRIC_DURATION, "bolt-1", "bolt-2"))
        .thenReturn(metrics);

    ExecuteCountSensor executeCountSensor
        = new ExecuteCountSensor(topologyProvider, null, metricsProvider);
    executeCountSensor.fetchMetrics();
    ComponentMetrics componentMetrics = executeCountSensor.getMetrics();
    assertEquals(2, componentMetrics.getComponentCount());

    Optional<InstanceMetrics> instanceMetrics
        = componentMetrics.getMetrics("bolt-1", "container_1_bolt-1_1", metric);
    assertEquals(123, instanceMetrics.get().getValueSum().intValue());

    instanceMetrics
        = componentMetrics.getMetrics("bolt-1", "container_1_bolt-1_2", metric);
    assertEquals(345, instanceMetrics.get().getValueSum().intValue());

    instanceMetrics
        = componentMetrics.getMetrics("bolt-2", "container_1_bolt-2_3", metric);
    assertEquals(321, instanceMetrics.get().getValueSum().intValue());

    instanceMetrics
        = componentMetrics.getMetrics("bolt-2", "container_1_bolt-2_4", metric);
    assertEquals(543, instanceMetrics.get().getValueSum().intValue());
  }
}
