package com.adobe.s3fs.metrics.data.filesystem;

import com.adobe.s3fs.metrics.data.S3FsFilesCreatedMetricsSource;
import com.adobe.s3fs.metrics.data.S3FsFilesDeletedMetricsSource;
import com.adobe.s3fs.metrics.data.S3FsFilesOverwrittenMetricsSource;
import com.adobe.s3fs.metrics.data.S3FsFilesRenamedMetricsSource;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(strictBuilder = true, typeImmutable = "*")
public abstract class AbstractFileSystemClientMetricsFactory {

  @Value.Default
  protected S3FsFilesCreatedMetricsSource createMetrics() {
    return S3FsFilesCreatedMetricsSource.getInstance();
  }

  @Value.Default
  protected S3FsFilesOverwrittenMetricsSource overwriteMetrics() {
    return S3FsFilesOverwrittenMetricsSource.getInstance();
  }

  @Value.Default
  protected S3FsFilesDeletedMetricsSource deleteMetrics() {
    return S3FsFilesDeletedMetricsSource.getInstance();
  }

  @Value.Default
  protected S3FsFilesRenamedMetricsSource renameMetrics() {
    return S3FsFilesRenamedMetricsSource.getInstance();
  }

  public FileSystemClientMetrics createFileSystemClientMetrics() {
    return new FileSystemClientMetrics(
        createMetrics(), overwriteMetrics(), deleteMetrics(), renameMetrics());
  }
}
