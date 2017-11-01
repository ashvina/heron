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


import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import com.microsoft.dhalion.metrics.ComponentMetrics;

import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.proto.system.Common.Status;
import com.twitter.heron.proto.system.Common.StatusCode;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricInterval;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.IndividualMetric;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.IndividualMetric.IntervalValue;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse.TaskMetric;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricsCacheLocation;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MetricsCacheMetricsProviderTest {
  @Test
  public void provides1Comp2InstanceMetricsFromeMetricsCache() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp = "bolt";
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt_1")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("104")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481288)
                        .setEnd(1497481288)))))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt_2")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("12")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481228)
                        .setEnd(1497481228)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("2")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481348)
                        .setEnd(1497481348)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("3")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481168)
                        .setEnd(1497481168)))))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    ComponentMetrics metrics =
        spyMetricsProvider.getComponentMetrics(metric, Duration.ofSeconds(60), comp);

    assertEquals(1, metrics.getComponentCount());
    assertEquals(2, metrics.filterByComponent(comp).size());

    ComponentMetrics result =
        metrics.filterByInstance(comp, "container_1_bolt_1").filterByMetric(metric);
    assertEquals(1, result.size());
    assertEquals(104, result.getLoneInstanceMetrics().get().getValueSum().intValue());

    result =
        metrics.filterByInstance(comp, "container_1_bolt_2").filterByMetric(metric);
    assertEquals(1, result.size());
    assertEquals(17, result.getLoneInstanceMetrics().get().getValueSum().intValue());
  }

  @Test
  public void providesMultipleComponentMetricsFromMetricsCache() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp1 = "bolt-1";
    TopologyMaster.MetricResponse response1 = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt-1_2")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("104")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481288)
                        .setEnd(1497481288)))))
        .build();

    doReturn(response1).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp1, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    String comp2 = "bolt-2";
    TopologyMaster.MetricResponse response2 = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt-2_1")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("12")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481228)
                        .setEnd(1497481228)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("2")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481348)
                        .setEnd(1497481348)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("3")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481168)
                        .setEnd(1497481168)))))
        .build();

    doReturn(response2).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp2, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    ComponentMetrics metrics
        = spyMetricsProvider.getComponentMetrics(metric, Duration.ofSeconds(60), comp1, comp2);

    assertEquals(2, metrics.getComponentCount());
    assertEquals(1, metrics.filterByComponent(comp1).size());

    ComponentMetrics result =
        metrics.filterByInstance(comp1, "container_1_bolt-1_2").filterByMetric(metric);
    assertEquals(1, result.size());
    assertEquals(104, result.getLoneInstanceMetrics().get().getValueSum().intValue());

    assertEquals(1, metrics.filterByComponent(comp2).size());
    result =
        metrics.filterByInstance(comp2, "container_1_bolt-2_1").filterByMetric(metric);
    assertEquals(1, result.size());
    assertEquals(17, result.getLoneInstanceMetrics().get().getValueSum().intValue());
  }

  @Test
  public void parsesBackPressureMetric() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "__time_spent_back_pressure_by_compid/container_1_split_1";
    String comp = "__stmgr__";
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("stmgr-1")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("601")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(0)
                        .setEnd(0)))))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));
    ComponentMetrics metrics
        = spyMetricsProvider.getComponentMetrics(metric, Duration.ofSeconds(60), comp);

    assertEquals(1, metrics.getComponentCount());
    assertNotNull(metrics.filterByComponent(comp));
    assertEquals(1, metrics.filterByComponent(comp).size());

    metrics = metrics.filterByInstance(comp, "stmgr-1");
    assertEquals(601, metrics.getLoneInstanceMetrics().get().getValueSum().intValue());
  }

  @Test
  public void handleMissingData() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "dummy";
    String comp = "split";
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));
    ComponentMetrics metrics
        = spyMetricsProvider.getComponentMetrics(metric, Duration.ofSeconds(60), comp);

    assertEquals(0, metrics.getComponentCount());
  }

  private MetricsCacheMetricsProvider createMetricsProviderSpy() {
    MetricsCacheLocation location = MetricsCacheLocation.newBuilder()
        .setTopologyName("testTopo")
        .setTopologyId("topoId")
        .setHost("localhost")
        .setControllerPort(0)
        .setMasterPort(0)
        .build();

    SchedulerStateManagerAdaptor stateMgr = Mockito.mock(SchedulerStateManagerAdaptor.class);
    when(stateMgr.getMetricsCacheLocation("testTopo")).thenReturn(location);

    MetricsCacheMetricsProvider metricsProvider
        = new MetricsCacheMetricsProvider(stateMgr, "testTopo");

    MetricsCacheMetricsProvider spyMetricsProvider = spy(metricsProvider);
    spyMetricsProvider.setClock(new TestClock(70000));
    return spyMetricsProvider;
  }

  @Test
  public void testGetTimeLineMetrics() {
    MetricsCacheMetricsProvider spyMetricsProvider = createMetricsProviderSpy();

    String metric = "count";
    String comp = "bolt";
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.newBuilder()
        .setStatus(Status.newBuilder().setStatus(StatusCode.OK))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt_1")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("104")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481288)
                        .setEnd(1497481288)))))
        .addMetric(TaskMetric.newBuilder()
            .setInstanceId("container_1_bolt_2")
            .addMetric(IndividualMetric.newBuilder()
                .setName(metric)
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("12")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481228)
                        .setEnd(1497481228)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("2")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481348)
                        .setEnd(1497481348)))
                .addIntervalValues(IntervalValue.newBuilder()
                    .setValue("3")
                    .setInterval(MetricInterval.newBuilder()
                        .setStart(1497481168)
                        .setEnd(1497481168)))))
        .build();

    doReturn(response).when(spyMetricsProvider)
        .getMetricsFromMetricsCache(metric, comp, Instant.ofEpochSecond(10), Duration.ofSeconds(60));

    ComponentMetrics metrics = spyMetricsProvider
        .getComponentMetrics(metric, Instant.ofEpochSecond(10), Duration.ofSeconds(60), comp);

    assertEquals(1, metrics.getComponentCount());
    ComponentMetrics compMetrics = metrics.filterByComponent(comp);
    assertEquals(2, compMetrics.size());

    ComponentMetrics instanceMetrics = compMetrics.filterByInstance(comp, "container_1_bolt_1");
    assertNotNull(instanceMetrics);
    assertEquals(1, instanceMetrics.size());

    Map<Instant, Double> metricValues = instanceMetrics.getLoneInstanceMetrics().get().getValues();
    assertEquals(1, metricValues.size());
    assertEquals(104, metricValues.get(Instant.ofEpochSecond(1497481288)).intValue());

    instanceMetrics = compMetrics.filterByInstance(comp, "container_1_bolt_2");
    assertNotNull(instanceMetrics);
    assertEquals(1, instanceMetrics.size());

    metricValues = instanceMetrics.getLoneInstanceMetrics().get().getValues();
    assertEquals(3, metricValues.size());
    assertEquals(12, metricValues.get(Instant.ofEpochSecond(1497481228L)).intValue());
    assertEquals(2, metricValues.get(Instant.ofEpochSecond(1497481348L)).intValue());
    assertEquals(3, metricValues.get(Instant.ofEpochSecond(1497481168L)).intValue());
  }

  private class TestClock extends MetricsCacheMetricsProvider.Clock {
    long timeStamp;

    TestClock(long timeStamp) {
      this.timeStamp = timeStamp;
    }

    @Override
    long currentTime() {
      return timeStamp;
    }
  }
}
