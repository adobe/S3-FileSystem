package com.adobe.s3fs.metrics.data;

import com.adobe.s3fs.metastore.api.ObjectLevelMetrics;
import com.adobe.s3fs.metrics.S3FsMetricsSystemFactory;
import com.google.common.base.Suppliers;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.util.function.Supplier;

@Metrics(context = "ObjectLevelMetrics")
public class ObjectLevelMetricsSource implements ObjectLevelMetrics {
  // Name of the metrics group
  private static final String METRICS_NAME = "ObjectLevelMetrics";
  // Brief description about this group
  private static final String METRICS_DESCRIPTION = "Metrics about errors at operation log & DynamoDB level";
  // The name of the metrics context that metrics will be under in jmx
  private static final String METRICS_JMX_CONTEXT = "FileSystem,sub=" + METRICS_NAME;

  // Failed pending op log key names
  private static final String FILES_FAILED_PENDING_LOG_NAME =
      "FailedPendingOpLog";
  private static final String FILES_FAILED_PENDING_LOG_DESC =
      "# of failed operations due to pending op log";
  // Failed commit op log key names
  private static final String FILES_FAILED_COMMIT_LOG_NAME =
      "FailedCommitOpLog";
  private static final String FILES_FAILED_COMMIT_LOG_DESC =
      "# of failed operations due to commit op log";
  // Failed DynamoDB key names
  private static final String FILES_FAILED_DYNAMODB_NAME = "FailedDynamoDB";
  private static final String FILES_FAILED_DYNAMODB_DESC =
      "# of failed operations due to DynamoDB";

  private static final String OP_LOG_SUCCESSFUL_ROLLBACK_NAME = "OpLogSuccessfulRollback";
  private static final String OP_LOG_SUCCESSFUL_ROLLBACK_DESC = "# of successful rollbacks of the operation log when " +
      "the operation fails in the metadata store";

  private static final String OP_LOG_FAILED_ROLLBACK_NAME = "OpLogFailedRollback";
  private static final String OP_LOG_FAILED_ROLLBACK_DESC = "# of failed rollbacks of the operation log when " +
      "the operation fails in the metadata store";

  // Number of failed operations due to pending log operation
  @Metric(value = {FILES_FAILED_PENDING_LOG_NAME, FILES_FAILED_PENDING_LOG_DESC})
  private MutableCounterLong failedPendingOpLog;
  // Number of failed operations due to commit log operation
  @Metric(value = {FILES_FAILED_COMMIT_LOG_NAME, FILES_FAILED_COMMIT_LOG_DESC})
  private MutableCounterLong failedCommitOpLog;
  // Number of failed operations due to DynamoDB operation
  @Metric(value = {FILES_FAILED_DYNAMODB_NAME, FILES_FAILED_DYNAMODB_DESC})
  private MutableCounterLong filesFailedDynamoDB;

  @Metric(value = {OP_LOG_SUCCESSFUL_ROLLBACK_NAME, OP_LOG_SUCCESSFUL_ROLLBACK_DESC})
  private MutableCounterLong successfulOpLogRollback;

  @Metric(value = {OP_LOG_FAILED_ROLLBACK_NAME, OP_LOG_FAILED_ROLLBACK_DESC})
  private MutableCounterLong failedOpLogRollback;

  // Supplier of ObjectLevelMetricsSource
  private static final Supplier<ObjectLevelMetricsSource> objectLevelMetricsSupplier =
      Suppliers.memoize(ObjectLevelMetricsSource::createInstance);
  // A repo of metrics which groups the metrics from this class
  private final MetricsRegistry registry = new MetricsRegistry(METRICS_NAME); //NOSONAR


  private ObjectLevelMetricsSource() {
    // Nothing to do
  }

  private static ObjectLevelMetricsSource createInstance() {
    ObjectLevelMetricsSource instance = new ObjectLevelMetricsSource();
    // Register instance to the JMX system
    S3FsMetricsSystemFactory.getS3FsMetricsSystem()
        .registerSource(METRICS_JMX_CONTEXT, METRICS_DESCRIPTION, instance);
    return instance;
  }

  public static ObjectLevelMetricsSource getInstance() {
    return objectLevelMetricsSupplier.get();
  }

  public void incrFailedPendingOpLog() {
    failedPendingOpLog.incr();
  }

  public void incrFailedCommitOpLog() {
    failedCommitOpLog.incr();
  }

  public void incrFailedDynamoDB() {
    filesFailedDynamoDB.incr();
  }

  @Override
  public void incrOpLogFailedRollback() { failedOpLogRollback.incr();}

  @Override
  public void incrOpLogSuccessfulRollback() { successfulOpLogRollback.incr();}
}
