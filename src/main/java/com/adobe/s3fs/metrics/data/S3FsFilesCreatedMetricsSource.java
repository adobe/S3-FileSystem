package com.adobe.s3fs.metrics.data;

import com.adobe.s3fs.metrics.S3FsMetricsSystemFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.util.function.Supplier;

@Metrics(context = "FilesCreated")
public class S3FsFilesCreatedMetricsSource {
  // Name of the metrics group
  private static final String METRICS_NAME = "FilesCreated";
  // Brief description about this group
  private static final String METRICS_DESCRIPTION = "Metrics about statuses of the files created";
  // The name of the metrics context that metrics will be under in jmx
  private static final String METRICS_JMX_CONTEXT = "FileSystem,sub=" + METRICS_NAME;

  private static final String CREATE_FILES_SUCCESSFUL_NAME = "CreatedFilesSuccessful";
  private static final String CREATE_FILES_SUCCESSFUL_DESC = "# of successful files created";
  private static final String CREATE_FILES_FAILED_NAME = "CreatedFilesFailed";
  private static final String CREATE_FILES_FAILED_DESC = "# of failed files created";

  // Below fields are created dynamically via reflection in MetricsSourceBuilder
  // Number of files created successful
  @Metric(value = {CREATE_FILES_SUCCESSFUL_NAME, CREATE_FILES_SUCCESSFUL_DESC})
  private MutableCounterLong createdFilesSuccessful;
  @Metric(value = {CREATE_FILES_FAILED_NAME, CREATE_FILES_FAILED_DESC})
  private MutableCounterLong createdFilesFailed;

  private static final Supplier<S3FsFilesCreatedMetricsSource> s3FsFilesCreatedSupplier =
      Suppliers.memoize(S3FsFilesCreatedMetricsSource::createInstance);
  // A repo of metrics which groups the metrics from this class
  // Tagging it with NOSONAR because the below field is used via Reflection
  // in Hadoop Metrics2 in MetricsSourceBuilder
  private final MetricsRegistry registry = new MetricsRegistry(METRICS_NAME); // NOSONAR

  private S3FsFilesCreatedMetricsSource() {
    // Nothing to do
  }

  private static S3FsFilesCreatedMetricsSource createInstance() {
    S3FsFilesCreatedMetricsSource instance = new S3FsFilesCreatedMetricsSource();
    // Register instance to the JMX system
    S3FsMetricsSystemFactory.getS3FsMetricsSystem()
        .registerSource(METRICS_JMX_CONTEXT, METRICS_DESCRIPTION, instance);
    return instance;
  }

  /** @return A cached instance of S3FsFilesCreatedMetricsSource */
  public static S3FsFilesCreatedMetricsSource getInstance() {
    return s3FsFilesCreatedSupplier.get();
  }

  @VisibleForTesting
  public static S3FsFilesCreatedMetricsSource getNewInstance() {
    return new S3FsFilesCreatedMetricsSource();
  }

  public void incrCreatedFilesSuccessful() {
    createdFilesSuccessful.incr();
  }

  public void incrCreatedFilesFailed() {
    createdFilesFailed.incr();
  }
}
