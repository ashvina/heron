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

package com.twitter.heron.metricsmgr.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.time.LocalTime;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;
import com.twitter.heron.spi.metricsmgr.sink.IMetricsSink;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

public class GranularSink implements IMetricsSink {
  private static final String KEY_METRICS_TYPE = "metrics-type";
  private static final String FILE_PATH_KEY = "output-filepath";
  private static final String FLUSH_COUNT = "flush-count";
  private static final String RECORD_PROCESS_COUNT = "record-process-count";

  // We would convert a file's metrics into a JSON object, i.e. array
  // So we need to add "[" at the start and "]" at the end
  private PrintStream writer;
  private SinkContext sinkContext;
  private final MetricsFilter metricsFilter = new MetricsFilter();
  private String regex = "container\\_(\\d+)\\_(.+)\\_(\\d+)";
  private Pattern p = Pattern.compile(regex);

  @Override
  @SuppressWarnings("unchecked")
  public void init(Map<String, Object> conf, SinkContext context) {
    verifyConf(conf);
    String topologyName = context.getTopologyName();
    sinkContext = context;

    Map<String, String> metricsType = (Map<String, String>) conf.get(KEY_METRICS_TYPE);
    if (metricsType != null) {
      for (String metric : metricsType.keySet()) {
        metricsFilter.setPrefixToType(metric, MetricsFilter.MetricAggregationType.UNKNOWN);
      }
    }

    String filePath = String.format("%s-%s-%s",
        conf.get(FILE_PATH_KEY), topologyName, context.getMetricsMgrId());
    writer = openNewFile(filePath);
  }

  private void verifyConf(Map<String, Object> conf) {
    if (!conf.containsKey(FILE_PATH_KEY)) {
      throw new IllegalArgumentException("Require: " + FILE_PATH_KEY);
    }
  }

  private PrintStream openNewFile(String filename) {
    // If the file already exists, set it Writable to avoid permission denied
    File f = new File(filename);
    if (f.exists() && !f.isDirectory()) {
      f.setWritable(true);
    }

    try {
      return new PrintStream(new FileOutputStream(filename, true), true, "UTF-8");
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      throw new RuntimeException("Error creating " + filename, e);
    }
  }

  @Override
  public void processRecord(MetricsRecord record) {
    Matcher m = p.matcher(record.getSource());
    m.find();
    String containerId = m.group(1);
    String component = m.group(2);
    String instanceId = m.group(3);
    for (MetricsInfo metricsInfo : record.getMetrics()) {
      if (!metricsFilter.contains(metricsInfo.getName())) {
        continue;
      }

      String msg = String.format("%s, %s, %s, %s, %s, %s",
          LocalTime.now().toString(),
          containerId, instanceId,
          component,
          metricsInfo.getName(),
          metricsInfo.getValue());
      writer.println(msg);
    }

    sinkContext.exportCountMetric(RECORD_PROCESS_COUNT, 1);
  }

  @Override
  public void flush() {
    writer.flush();

    // Update the Metrics
    sinkContext.exportCountMetric(FLUSH_COUNT, 1);
  }

  @Override
  public void close() {
    if (writer != null) {
      writer.close();
    }
  }
}
