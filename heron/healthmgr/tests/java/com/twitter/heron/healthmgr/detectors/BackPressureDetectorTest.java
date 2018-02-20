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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;

import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.Symptom;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;

import static com.twitter.heron.healthmgr.detectors.BackPressureDetector.CONF_NOISE_FILTER;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackPressureDetectorTest {
  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_NOISE_FILTER, 20)).thenReturn(50);


    Measurement measurement1
        = new Measurement("bolt", "i1", METRIC_BACK_PRESSURE.text(), Instant.now(), 55);
    Measurement measurement2
        = new Measurement("bolt", "i2", METRIC_BACK_PRESSURE.text(), Instant.now(), 3);
    Measurement measurement3
        = new Measurement("bolt", "i3", METRIC_BACK_PRESSURE.text(), Instant.now(), 0);
    Collection<Measurement> metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);
    metrics.add(measurement3);

    BackPressureDetector detector = new BackPressureDetector(config);
    Collection<Symptom> symptoms = detector.detect(metrics);

    Assert.assertEquals(1, symptoms.size());
    Symptom symptom = symptoms.iterator().next();
    Assert.assertEquals(1, symptom.assignments().size());

    measurement1
        = new Measurement("bolt", "i1", METRIC_BACK_PRESSURE.text(), Instant.now(), 45);
    measurement2
        = new Measurement("bolt", "i2", METRIC_BACK_PRESSURE.text(), Instant.now(), 3);
    metrics = new ArrayList<>();
    metrics.add(measurement1);
    metrics.add(measurement2);

    detector = new BackPressureDetector(config);
    symptoms = detector.detect(metrics);

    Assert.assertEquals(0, symptoms.size());
  }
}
