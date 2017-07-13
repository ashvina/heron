
load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")

healthmgr_deps_files = [
    "//heron/healthm/src/java:healthmgr-java",
    "//heron/api/src/java:api-java",
    "//heron/api/src/java:classification",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//heron/common/src/java:utils-java",
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:utils-spi-java",
    "//heron/spi/src/java:metricsmgr-spi-java",
    "//heron/proto:proto_common_java",
    "//heron/proto:proto_metrics_java",
    "//heron/proto:proto_packing_plan_java",
    "//heron/proto:proto_scheduler_java",
    "//heron/proto:proto_tmaster_java",
    "//heron/proto:proto_topology_java",
    "//heron/metricsmgr/src/thrift:thrift_scribe_java",
    "//heron/scheduler-core/src/java:scheduler-java",
    "//third_party/java:jackson",
    "@com_google_protobuf_protobuf_java//jar",
    "@commons_cli_commons_cli//jar",
    "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider//jar",
    "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_base//jar",
    "@com_fasterxml_jackson_module_jackson_module_jaxb_annotations//jar",
    "@com_jayway_jsonpath//jar",
    "@net_minidev_json_smart//jar",
    "@javax_ws_rs_javax_ws_rs_api//jar",
    "@javax_annotation_javax_annotation_api//jar",
    "@javax_inject_javax_inject//jar",
    "@org_apache_thrift_libthrift//jar",
    "@org_glassfish_jersey_core_jersey_client//jar",
    "@org_glassfish_jersey_core_jersey_common//jar",
    "@org_glassfish_jersey_ext_jersey_entity_filtering//jar",
    "@org_glassfish_jersey_media_jersey_media_json_jackson//jar",
    "@org_glassfish_jersey_bundles_repackaged_jersey_guava//jar",
    "@org_glassfish_hk2_hk2_api//jar",
    "@org_glassfish_hk2_hk2_utils//jar",
    "@org_glassfish_hk2_hk2_locator//jar",
    "@org_yaml_snakeyaml//jar",
    "@com_google_guava_guava//jar",
    "@com_google_inject_guice//jar",
    "@com_google_inject_extensions_guice_assistedinject//jar",
    "@com_microsoft_dhalion//jar",
    "@aopalliance_aopalliance//jar",
    "@org_apache_commons_commons_math3//jar",
]

test_deps_files = [
    "//third_party/java:junit4",
    "//third_party/java:mockito",
]


java_library(
    name = "healthmgr-utils",
    srcs = glob(
        ["com/twitter/heron/healthmgr/*.java"]
    ),
    deps =  healthmgr_deps_files + \
               test_deps_files,
)

java_tests(
   test_classes = [
       "com.twitter.heron.healthmgr.common.PackingPlanProviderTest",
       "com.twitter.heron.healthmgr.common.TopologyProviderTest",
       "com.twitter.heron.healthmgr.detectors.BackPressureDetectorTest",
       "com.twitter.heron.healthmgr.detectors.GrowingWaitQueueDetectorTest",
       "com.twitter.heron.healthmgr.detectors.LargeWaitQueueDetectorTest",
       "com.twitter.heron.healthmgr.detectors.ProcessingRateSkewDetectorTest",
       "com.twitter.heron.healthmgr.detectors.SmallWaitQueueDetectorTest",
       "com.twitter.heron.healthmgr.detectors.WaitQueueDisparityDetectorTest",
       "com.twitter.heron.healthmgr.diagnosers.DataSkewDiagnoserTest",
       "com.twitter.heron.healthmgr.diagnosers.SlowInstanceDiagnoserTest",
       "com.twitter.heron.healthmgr.diagnosers.UnderProvisioningDiagnoserTest",
       "com.twitter.heron.healthmgr.sensors.BackPressureSensorTest",
       "com.twitter.heron.healthmgr.sensors.BufferSizeSensorTest",
       "com.twitter.heron.healthmgr.sensors.ExecuteCountSensorTest",
       "com.twitter.heron.healthmgr.sensors.TrackerMetricsProviderTest",
       "com.twitter.heron.healthmgr.HealthManagerTest",
       "com.twitter.heron.healthmgr.HealthPolicyConfigReaderTest",
       "com.twitter.heron.healthmgr.resolvers.ScaleDownResolverTest",
       "com.twitter.heron.healthmgr.resolvers.ScaleUpResolverTest",
     ],
     runtime_deps = [ ":healthmgr-utils" ],
     size = "small",
)


