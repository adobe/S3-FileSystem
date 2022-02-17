package com.adobe.s3fs.metrics.data.filesystem;

import com.adobe.s3fs.filesystem.FileSystemMetrics;
import com.adobe.s3fs.filesystem.QualifyingFileSystemMetrics;
import com.adobe.s3fs.metrics.data.S3FsFilesCreatedMetricsSource;
import com.adobe.s3fs.metrics.data.S3FsFilesDeletedMetricsSource;
import com.adobe.s3fs.metrics.data.S3FsFilesOverwrittenMetricsSource;
import com.adobe.s3fs.metrics.data.S3FsFilesRenamedMetricsSource;

import java.util.Objects;

public class FileSystemClientMetrics implements FileSystemMetrics, QualifyingFileSystemMetrics {

  private final S3FsFilesCreatedMetricsSource createMetrics;
  private final S3FsFilesOverwrittenMetricsSource overwriteMetrics;
  private final S3FsFilesDeletedMetricsSource deleteMetrics;
  private final S3FsFilesRenamedMetricsSource renameMetrics;

  protected FileSystemClientMetrics(
      S3FsFilesCreatedMetricsSource createMetrics,
      S3FsFilesOverwrittenMetricsSource overwriteMetrics,
      S3FsFilesDeletedMetricsSource deleteMetrics,
      S3FsFilesRenamedMetricsSource renameMetrics) {
    this.createMetrics = Objects.requireNonNull(createMetrics);
    this.overwriteMetrics = Objects.requireNonNull(overwriteMetrics);
    this.deleteMetrics = Objects.requireNonNull(deleteMetrics);
    this.renameMetrics = Objects.requireNonNull(renameMetrics);
  }

  @Override
  public void recordSuccessfulCreate() {
    createMetrics.incrCreatedFilesSuccessful();
  }

  @Override
  public void recordSuccessfulOverwrite() {
    overwriteMetrics.incrOverwriteFilesSuccessful();
  }

  @Override
  public void recordFailedPhysicalDelete() {
    deleteMetrics.incrDeletedFilesFailedS3();
  }

  @Override
  public void recordFailedCreate() {
    createMetrics.incrCreatedFilesFailed();
  }

  @Override
  public void recordFailedOverwrite() {
    overwriteMetrics.incrOverwriteFilesFailed();
  }

  @Override
  public void recordSuccessfulRename() {
    renameMetrics.incrRenameFilesSuccessful();
  }

  @Override
  public void recordFailedRename() {
    renameMetrics.incrRenameFilesFailed();
  }

  @Override
  public void recordSuccessfulDelete() {
    deleteMetrics.incrDeletedFilesSuccessful();
  }

  @Override
  public void recordFailedDelete() {
    deleteMetrics.incrDeletedFilesFailed();
  }

  @Override
  public void recordSuccessfulMkdir() {
    // At this moment don't make any difference between a directory and a file
    // Increment the metrics for successful files created if a directory
    // is created successfully
    createMetrics.incrCreatedFilesSuccessful();
  }

  @Override
  public void recordFailedMkdir() {
    // At this moment don't make any difference between a directory and a file
    // Increment the metrics for failed files if a directory creation fails
    createMetrics.incrCreatedFilesFailed();
  }

  @Override
  public void recordFailedOverwrittenFilePhysicalDelete() {
    overwriteMetrics.incrOverwriteFilesPhysicalDeleteFailed();
  }
}
