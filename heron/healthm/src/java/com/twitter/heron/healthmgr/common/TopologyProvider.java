// Copyright 2016 Microsoft. All rights reserved.
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

package com.twitter.heron.healthmgr.common;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

/**
 * A topology may be updated after initial deployment. This provider is used to provide the latest
 * version to any dependent components.
 */
@Singleton
public class TopologyProvider implements Provider<Topology> {
  private static final Logger LOG = Logger.getLogger(TopologyProvider.class.getName());
  private final SchedulerStateManagerAdaptor stateManagerAdaptor;
  private final String topologyName;

  private Topology topology;

  @Inject
  public TopologyProvider(SchedulerStateManagerAdaptor stateManagerAdaptor,
                          @Named(HealthMgrConstants.CONF_TOPOLOGY_NAME) String topologyName) {
    this.stateManagerAdaptor = stateManagerAdaptor;
    this.topologyName = topologyName;
  }

  @Override
  public synchronized Topology get() {
    if (topology == null) {
      fetchLatestTopology();
    }
    return topology;
  }

  private synchronized void fetchLatestTopology() {
    LOG.log(Level.INFO, "Fetching topology from state manager: {0}", topologyName);
    this.topology = stateManagerAdaptor.getTopology(topologyName);
    if (topology == null) {
      throw new RuntimeException("Failed to fetch topology from the State Manager");
    }
  }

  /**
   * Invalidates cached topology instance on receiving update notification
   */
  public synchronized void onNext(HealthManagerEvents.TOPOLOGY_UPDATE event) {
    LOG.info("Received topology update event, invalidating cached topology: " + event);
    this.topology = null;
  }

  /**
   * A utility method to extract bolt component names from the topology.
   *
   * @return array of all bolt names
   */
  public String[] getBoltNames() {
    Topology topology = get();
    ArrayList<String> boltNames = new ArrayList<>();
    for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
      boltNames.add(bolt.getComp().getName());
    }

    return boltNames.toArray(new String[boltNames.size()]);
  }
}