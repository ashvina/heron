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

package com.twitter.heron.healthmgr.resolvers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.resolver.Action;

import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.StatsCollector;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.scheduler.Scheduler.UpdateTopologyRequest;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.TopologyTests;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_OVER_PROVISIONING_SMALLWAITQ;
import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_OVER_PROVISIONING_UNSATCOMP;
import static com.twitter.heron.healthmgr.resolvers.ScaleDownResolver.CONF_SCALE_DOWN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScaleDownResolverTest {
  static final String EXE_COUNT = HealthMgrConstants.METRIC_EXE_COUNT;
  static final String BUFFER_SIZE = HealthMgrConstants.METRIC_BUFFER_SIZE;

  private EventManager eventManager = new EventManager();

  @Test
  public void testResolveSmallWaitQ() {
    StatsCollector statsCollector = new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double) 1000)).when(spyCollector).getProcessingRateStats("bolt");

    HealthPolicyConfig healthconfig = mock(HealthPolicyConfig.class);
    when(healthconfig.getConfig(CONF_SCALE_DOWN, "5")).thenReturn("10");

    TopologyAPI.Topology topology = createTestTopology();
    Config config = createConfig(topology);
    PackingPlan currentPlan = createPacking(topology, config);

    PackingPlanProvider packingPlanProvider = mock(PackingPlanProvider.class);
    when(packingPlanProvider.get()).thenReturn(currentPlan);

    ISchedulerClient scheduler = mock(ISchedulerClient.class);
    when(scheduler.updateTopology(any(UpdateTopologyRequest.class))).thenReturn(true);

    ComponentMetrics metrics = new ComponentMetrics("bolt-1", "i1", BUFFER_SIZE, 1);
    metrics.addInstanceMetric(new InstanceMetrics("i2", BUFFER_SIZE, 1));

    Symptom symptom = new Symptom(SYMPTOM_OVER_PROVISIONING_SMALLWAITQ, metrics);
    List<Diagnosis> diagnosis = new ArrayList<>();
    diagnosis.add(new Diagnosis("test", symptom));

    ScaleDownResolver resolver
        = new ScaleDownResolver(null, packingPlanProvider, scheduler, eventManager, null,
        healthconfig, spyCollector);
    ScaleDownResolver spyResolver = spy(resolver);

    doReturn(2).when(spyResolver).computeScaleDownFactor(metrics, symptom.getName());
    doReturn(currentPlan).when(spyResolver).buildNewPackingPlan(any(HashMap.class), eq(currentPlan));

    List<Action> result = spyResolver.resolve(diagnosis);
    verify(scheduler, times(1)).updateTopology(any(UpdateTopologyRequest.class));
    assertEquals(1, result.size());
  }

  @Test
  public void testResolveUnsatComponent() {
    StatsCollector statsCollector = new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double) 1000)).when(spyCollector).getProcessingRateStats("bolt");

    HealthPolicyConfig healthconfig = mock(HealthPolicyConfig.class);
    when(healthconfig.getConfig(CONF_SCALE_DOWN, "5")).thenReturn("10");

    TopologyAPI.Topology topology = createTestTopology();
    Config config = createConfig(topology);
    PackingPlan currentPlan = createPacking(topology, config);

    PackingPlanProvider packingPlanProvider = mock(PackingPlanProvider.class);
    when(packingPlanProvider.get()).thenReturn(currentPlan);

    ISchedulerClient scheduler = mock(ISchedulerClient.class);
    when(scheduler.updateTopology(any(UpdateTopologyRequest.class))).thenReturn(true);

    ComponentMetrics metrics = new ComponentMetrics("bolt-1", "i1", EXE_COUNT, 100);
    metrics.addInstanceMetric(new InstanceMetrics("i2", EXE_COUNT, 150));

    Symptom symptom = new Symptom(SYMPTOM_OVER_PROVISIONING_UNSATCOMP, metrics);
    List<Diagnosis> diagnosis = new ArrayList<>();
    diagnosis.add(new Diagnosis("test", symptom));

    ScaleDownResolver resolver
        = new ScaleDownResolver(null, packingPlanProvider, scheduler, eventManager, null,
        healthconfig, spyCollector);
    ScaleDownResolver spyResolver = spy(resolver);

    doReturn(2).when(spyResolver).computeScaleDownFactor(metrics, symptom.getName());
    doReturn(currentPlan).when(spyResolver).buildNewPackingPlan(any(HashMap.class), eq(currentPlan));

    List<Action> result = spyResolver.resolve(diagnosis);
    verify(scheduler, times(1)).updateTopology(any(UpdateTopologyRequest.class));
    assertEquals(1, result.size());
  }

  @Test
  public void testResolveNoSymptom() {
    StatsCollector statsCollector = new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double) 1000)).when(spyCollector).getProcessingRateStats("bolt");

    HealthPolicyConfig healthconfig = mock(HealthPolicyConfig.class);
    when(healthconfig.getConfig(CONF_SCALE_DOWN, "5")).thenReturn("10");

    TopologyAPI.Topology topology = createTestTopology();
    Config config = createConfig(topology);
    PackingPlan currentPlan = createPacking(topology, config);

    PackingPlanProvider packingPlanProvider = mock(PackingPlanProvider.class);
    when(packingPlanProvider.get()).thenReturn(currentPlan);

    ISchedulerClient scheduler = mock(ISchedulerClient.class);
    when(scheduler.updateTopology(any(UpdateTopologyRequest.class))).thenReturn(true);

    ComponentMetrics metrics = new ComponentMetrics("bolt-1", "i1", EXE_COUNT, 100);
    metrics.addInstanceMetric(new InstanceMetrics("i2", EXE_COUNT, 150));

    Symptom symptom = new Symptom("RANDOM_SYMPTOM", metrics);
    List<Diagnosis> diagnosis = new ArrayList<>();
    diagnosis.add(new Diagnosis("test", symptom));

    ScaleDownResolver resolver
        = new ScaleDownResolver(null, packingPlanProvider, scheduler, eventManager, null,
        healthconfig, spyCollector);
    ScaleDownResolver spyResolver = spy(resolver);

    List<Action> result = spyResolver.resolve(diagnosis);
    assertNull(result);
  }

  @Test
  public void testBuildPackingPlan() {
    TopologyAPI.Topology topology = createTestTopology();
    TopologyProvider topologyProvider = createTopologyProvider(topology);
    Config config = createConfig(topology);
    PackingPlan currentPlan = createPacking(topology, config);

    StatsCollector statsCollector = new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double) 1000)).when(spyCollector).getProcessingRateStats("bolt");

    HealthPolicyConfig healthconfig = mock(HealthPolicyConfig.class);
    when(healthconfig.getConfig(CONF_SCALE_DOWN, "5")).thenReturn("50");

    Map<String, Integer> changeRequest = new HashMap<>();
    changeRequest.put("bolt-1", 1);

    Map<String, Integer> deltaChange = new HashMap<>();
    deltaChange.put("bolt-1", -1);

    IRepacking repacking = mock(IRepacking.class);
    when(repacking.repack(currentPlan, deltaChange)).thenReturn(currentPlan);

    ScaleDownResolver resolver =
        new ScaleDownResolver(topologyProvider, null, null, eventManager, config,
            healthconfig, spyCollector);
    ScaleDownResolver spyResolver = spy(resolver);
    doReturn(repacking).when(spyResolver).getRepackingClass("Repacking");

    PackingPlan newPlan = spyResolver.buildNewPackingPlan(changeRequest, currentPlan);
    assertEquals(currentPlan, newPlan);
  }

  private PackingPlan createPacking(TopologyAPI.Topology topology, Config config) {
    RoundRobinPacking packing = new RoundRobinPacking();
    packing.initialize(config, topology);
    return packing.pack();
  }

  private Config createConfig(TopologyAPI.Topology topology) {
    return Config.newBuilder(true)
        .put(Key.TOPOLOGY_ID, topology.getId())
        .put(Key.TOPOLOGY_NAME, topology.getName())
        .put(Key.REPACKING_CLASS, "Repacking")
        .build();
  }

  private TopologyProvider createTopologyProvider(TopologyAPI.Topology topology) {
    TopologyProvider topologyProvider = mock(TopologyProvider.class);
    when(topologyProvider.get()).thenReturn(topology);
    return topologyProvider;
  }

  private TopologyAPI.Topology createTestTopology() {
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt-1", 2);
    bolts.put("bolt-2", 1);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", 1);
    return TopologyTests.createTopology("T", new com.twitter.heron.api.Config(), spouts, bolts);
  }

  @Test
  public void testScaleDownFactorComputation() {
    StatsCollector statsCollector = new StatsCollector();
    StatsCollector spyCollector = spy(statsCollector);
    doReturn(Optional.of((double) 200)).when(spyCollector).getProcessingRateStats("bolt");

    HealthPolicyConfig healthconfig = mock(HealthPolicyConfig.class);
    when(healthconfig.getConfig(CONF_SCALE_DOWN, "5")).thenReturn("50");

    ScaleDownResolver resolver =
        new ScaleDownResolver(null, null, null, eventManager, null,
            healthconfig, spyCollector);

    ComponentMetrics metrics = new ComponentMetrics("bolt");
    metrics.addInstanceMetric(new InstanceMetrics("i1", BUFFER_SIZE, 1));
    metrics.addInstanceMetric(new InstanceMetrics("i2", BUFFER_SIZE, 1));

    int result = resolver.computeScaleDownFactor(metrics, SYMPTOM_OVER_PROVISIONING_SMALLWAITQ);
    assertEquals(1, result);

    metrics = new ComponentMetrics("bolt");
    metrics.addInstanceMetric(new InstanceMetrics("i1", EXE_COUNT, 100));
    metrics.addInstanceMetric(new InstanceMetrics("i2", EXE_COUNT, 100));
    metrics.addInstanceMetric(new InstanceMetrics("i3", EXE_COUNT, 100));

    result = resolver.computeScaleDownFactor(metrics, SYMPTOM_OVER_PROVISIONING_UNSATCOMP);
    assertEquals(2, result);
  }
}
