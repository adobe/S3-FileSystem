package com.adobe.s3fs.metrics.data;

import com.adobe.s3fs.metrics.S3FsMetricsSystemFactory;
import com.google.common.base.Suppliers;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.util.function.Supplier;

@Metrics(context = "FilesOverwritten")
public class S3FsFilesOverwrittenMetricsSource {
  // Name of the metrics group
  private static final String METRICS_NAME = "FilesOverwritten";
  // Brief description about this group
  private static final String METRICS_DESCRIPTION = "Metrics about statuses of files overwritten";
  // The name of the metrics context that metrics will be under in jmx
  private static final String METRICS_JMX_CONTEXT = "FileSystem,sub=" + METRICS_NAME;

  // Key names
  private static final String OVERWRITE_FILES_SUCCESSFUL_NAME = "OverwriteFilesSuccessful";
  private static final String OVERWRITE_FILES_SUCCESSFUL_DESC = "# of successful overwrite files";
  private static final String OVERWRITE_FILES_FAILED_NAME = "OverwriteFilesFailed";
  private static final String OVERWRITE_FILES_FAILED_DESC = "# of failed overwrite files";
  private static final String OVERWRITE_FILES_PHYSICAL_DELETE_FAILED_NAME = "OverwriteFileFailedPhysicalDeleteFailed";
  private static final String OVERWRITE_FILES_PHYSICAL_DELETE_FAILED_DESC = "# of overwritten files for which deletion of the physical file failed";

  // Number of successful overwrite files
  @Metric(value = {OVERWRITE_FILES_SUCCESSFUL_NAME, OVERWRITE_FILES_SUCCESSFUL_DESC})
  private MutableCounterLong overwriteFilesSuccessful;
  @Metric(value = {OVERWRITE_FILES_FAILED_NAME, OVERWRITE_FILES_FAILED_DESC})
  private MutableCounterLong overwriteFilesFailed;
  @Metric(value = {OVERWRITE_FILES_PHYSICAL_DELETE_FAILED_NAME, OVERWRITE_FILES_PHYSICAL_DELETE_FAILED_DESC})
  private MutableCounterLong overwriteFilesPhysicalDeleteFailed;

  // Supplier of S3FsFilesOverwrittenMetricsSource
  private static final Supplier<S3FsFilesOverwrittenMetricsSource> s3FsFilesOverwriteSupplier =
      Suppliers.memoize(S3FsFilesOverwrittenMetricsSource::createInstance);
  // A repo of metrics which groups the metrics from this class
  private final MetricsRegistry registry = new MetricsRegistry(METRICS_NAME); // NOSONAR

  private S3FsFilesOverwrittenMetricsSource() {
    // Nothing to do
  }

  private static S3FsFilesOverwrittenMetricsSource createInstance() {
    S3FsFilesOverwrittenMetricsSource instance = new S3FsFilesOverwrittenMetricsSource();
    // Register instance to the JMX system
    S3FsMetricsSystemFactory.getS3FsMetricsSystem()
        .registerSource(METRICS_JMX_CONTEXT, METRICS_DESCRIPTION, instance);
    return instance;
  }

  /** @return A cached instance of S3FsFilesOverwrittenMetricsSource */
  public static S3FsFilesOverwrittenMetricsSource getInstance() {
    return s3FsFilesOverwriteSupplier.get();
  }

  public void incrOverwriteFilesSuccessful() {
    overwriteFilesSuccessful.incr();
  }

  public void incrOverwriteFilesFailed() {
    overwriteFilesFailed.incr();
  }

  public void incrOverwriteFilesPhysicalDeleteFailed() { overwriteFilesPhysicalDeleteFailed.incr();}
}
