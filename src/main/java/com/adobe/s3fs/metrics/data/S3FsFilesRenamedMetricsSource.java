package com.adobe.s3fs.metrics.data;

import com.adobe.s3fs.metrics.S3FsMetricsSystemFactory;
import com.google.common.base.Suppliers;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.util.function.Supplier;

@Metrics(context = "FilesRenamed")
public class S3FsFilesRenamedMetricsSource {
  // Name of the metrics group
  private static final String METRICS_NAME = "FilesRenamed";
  // Brief description about this group
  private static final String METRICS_DESCRIPTION = "Metrics about statuses of renamed files";
  // The name of the metrics context that metrics will be under in jmx
  private static final String METRICS_JMX_CONTEXT = "FileSystem,sub=" + METRICS_NAME;

  // Successful Rename key names
  private static final String RENAME_FILES_SUCCESSFUL_NAME = "RenameFilesSuccessful";
  private static final String RENAME_FILES_SUCCESSFUL_DESC = "# of successful rename files";
  private static final String RENAME_FILES_FAILED_NAME = "RenameFilesFailed";
  private static final String RENAME_FILES_FAILED_DESC = "# of failed rename files";

  // Number of successful rename files
  @Metric(value = {RENAME_FILES_SUCCESSFUL_NAME, RENAME_FILES_SUCCESSFUL_DESC})
  private MutableCounterLong renameFilesSuccessful;
  @Metric(value = {RENAME_FILES_FAILED_NAME, RENAME_FILES_FAILED_DESC})
  private MutableCounterLong renameFilesFailed;

  // Supplier of S3FsFilesOverwrittenMetricsSource
  private static final Supplier<S3FsFilesRenamedMetricsSource> s3FsFilesOverwriteSupplier =
      Suppliers.memoize(S3FsFilesRenamedMetricsSource::createInstance);
  // A repo of metrics which groups the metrics from this class
  private final MetricsRegistry registry = new MetricsRegistry(METRICS_NAME); //NOSONAR

  private S3FsFilesRenamedMetricsSource() {
    // Nothing to do
  }

  private static S3FsFilesRenamedMetricsSource createInstance() {
    S3FsFilesRenamedMetricsSource instance = new S3FsFilesRenamedMetricsSource();
    // Register instance to the JMX system
    S3FsMetricsSystemFactory.getS3FsMetricsSystem()
        .registerSource(METRICS_JMX_CONTEXT, METRICS_DESCRIPTION, instance);
    return instance;
  }

  public static S3FsFilesRenamedMetricsSource getInstance() {
    return s3FsFilesOverwriteSupplier.get();
  }

  public void incrRenameFilesSuccessful() {
    renameFilesSuccessful.incr();
  }

  public void incrRenameFilesFailed() {
    renameFilesFailed.incr();
  }
}
