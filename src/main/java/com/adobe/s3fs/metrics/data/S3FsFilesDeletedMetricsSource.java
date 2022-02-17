package com.adobe.s3fs.metrics.data;

import com.adobe.s3fs.metrics.S3FsMetricsSystemFactory;
import com.google.common.base.Suppliers;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.util.function.Supplier;

@Metrics(context = "FilesDeleted")
public class S3FsFilesDeletedMetricsSource {
  // Name of the metrics group
  private static final String METRICS_NAME = "FilesDeleted";
  // Brief description about this group
  private static final String METRICS_DESCRIPTION = "Metrics about statuses of the deleted files";
  // The name of the metrics context that metrics will be under in jmx
  private static final String METRICS_JMX_CONTEXT = "FileSystem,sub=" + METRICS_NAME;

  private static final String DELETE_FILES_SUCCESSFUL_NAME = "DeleteFilesSuccessful";
  private static final String DELETE_FILES_SUCCESSFUL_DESC = "# of successful files deleted";
  private static final String DELETE_FILES_FAILED_NAME = "DeleteFilesFailed";
  private static final String DELETE_FILES_FAILED_DESC = "# of failed files deleted";
  private static final String DELETE_FILES_FAILED_S3_NAME = "DeleteFilesFailedS3";
  private static final String DELETE_FILES_FAILED_S3_DESC =
      "# of deleted failed files in S3";

  // Number of files deleted successful
  @Metric(value = {DELETE_FILES_SUCCESSFUL_NAME, DELETE_FILES_SUCCESSFUL_DESC})
  private MutableCounterLong deletedFilesSuccessful;
  @Metric(value = {DELETE_FILES_FAILED_NAME, DELETE_FILES_FAILED_DESC})
  private MutableCounterLong deletedFilesFailed;
  // Number of files failed during deletion due to S3
  @Metric(value = {DELETE_FILES_FAILED_S3_NAME, DELETE_FILES_FAILED_S3_DESC})
  private MutableCounterLong deletedFilesFailedS3;

  private static final Supplier<S3FsFilesDeletedMetricsSource> s3FsFilesDeletedSupplier =
      Suppliers.memoize(S3FsFilesDeletedMetricsSource::createInstance);
  // A repo of metrics which groups the metrics from this class
  // Tagging it with NOSONAR because the below field is used via Reflection
  // in Hadoop Metrics2 in MetricsSourceBuilder
  private final MetricsRegistry registry = new MetricsRegistry(METRICS_NAME); // NOSONAR

  private S3FsFilesDeletedMetricsSource() {
    // Nothing to do
  }

  private static S3FsFilesDeletedMetricsSource createInstance() {
    S3FsFilesDeletedMetricsSource instance = new S3FsFilesDeletedMetricsSource();
    // Register instance to the JMX system
    S3FsMetricsSystemFactory.getS3FsMetricsSystem()
        .registerSource(METRICS_JMX_CONTEXT, METRICS_DESCRIPTION, instance);
    return instance;
  }

  public static S3FsFilesDeletedMetricsSource getInstance() {
    return s3FsFilesDeletedSupplier.get();
  }

  public void incrDeletedFilesSuccessful() {
    deletedFilesSuccessful.incr();
  }

  public void incrDeletedFilesFailed() {
    deletedFilesFailed.incr();
  }

  public void incrDeletedFilesFailedS3() {
    deletedFilesFailedS3.incr();
  }
}
