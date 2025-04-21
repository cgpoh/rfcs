- Start Date: 2025-04-21
- RFC PR: https://github.com/datahub-project/rfcs/pull/11
- Discussion Issue: (GitHub issue this was discussed in before the RFC, if any)
- Implementation PR(s): (leave this empty)

# Spark Streaming Sink/Source Platform

## Summary

Allows configuration of Spark structured streaming sink and source platform.

## Motivation

The motivation for this RFC stems from an issue encountered while capturing data lineage using DataHub with Spark Structured Streaming. In the DataHub [code](https://github.com/datahub-project/datahub/blob/master/metadata-integration/java/acryl-spark-lineage/src/main/java/datahub/spark/converter/SparkStreamingEventToDatahub.java#L145), a regular expression matcher expects sources to be prefixed with identifiers like Kafka[…] to determine the data platform. However, since Iceberg tables lack such a prefix (e.g., iceberg[…]), DataHub fails to recognize the platform and thus shows no lineage.

## Requirements

- The proposal should be able to identify the data platform of a source or sink based on the configuration provided in the Spark job.

## Detailed design

It is proposed to introduce two new Spark configurations: `spark.datahub.streaming.source.platform` for specifying the streaming source platform, and `spark.datahub.streaming.sink.platform` for the streaming sink. Within the `generateUrnFromStreamingDescription` method in `SparkStreamingEventToDatahub.java`, these configurations will serve as fallbacks in cases where the regular expression matcher fails to extract the platform. If the configurations are set, their values will be used to determine the data platform. An example implementation is shown below:
```java
  public static Optional<DatasetUrn> generateUrnFromStreamingDescription(
        String description, SparkLineageConf sparkLineageConf) {
    return SparkStreamingEventToDatahub.generateUrnFromStreamingDescription(
            description, sparkLineageConf, false);
}

public static Optional<DatasetUrn> generateUrnFromStreamingDescription(
  String description, SparkLineageConf sparkLineageConf, boolean isSink) {
    String pattern = "(.*?)\\[(.*)]";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(description);
    if (m.find()) {
      String namespace = m.group(1);
      String platform = getDatahubPlatform(namespace);
      String path = m.group(2);
      log.debug("Streaming description Platform: {}, Path: {}", platform, path);
      if (platform.equals(KAFKA_PLATFORM)) {
        path = getKafkaTopicFromPath(m.group(2));
      } else if (platform.equals(FILE_PLATFORM) || platform.equals(DELTA_LAKE_PLATFORM)) {
        try {
          DatasetUrn urn =
                  HdfsPathDataset.create(new URI(path), sparkLineageConf.getOpenLineageConf()).urn();
          return Optional.of(urn);
        } catch (InstantiationException e) {
          return Optional.empty();
        } catch (URISyntaxException e) {
          log.error("Failed to parse path {}", path, e);
          return Optional.empty();
        }
      }
      return Optional.of(
              new DatasetUrn(
                      new DataPlatformUrn(platform),
                      path,
                      sparkLineageConf.getOpenLineageConf().getFabricType()));
    } else {
      if (sparkLineageConf.getOpenLineageConf().getStreamingSinkPlatform() != null && isSink) {
        return generateUrnFromStreamingDescription(
          description,
          sparkLineageConf,
          sparkLineageConf.getOpenLineageConf().getStreamingSinkPlatform()
        );
      } else if (sparkLineageConf.getOpenLineageConf().getStreamingSourcePlatform() != null && !isSink) {
        return generateUrnFromStreamingDescription(
          description,
          sparkLineageConf,
          sparkLineageConf.getOpenLineageConf().getStreamingSourcePlatform()
        );
      } else {
        return Optional.empty();
      }
    }
}
```