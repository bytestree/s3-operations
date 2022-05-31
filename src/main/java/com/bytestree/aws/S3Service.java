package com.bytestree.aws;

import software.amazon.awssdk.core.ResponseInputStream;

import java.nio.file.Path;

/**
 * @author bytestree
 */
public interface S3Service {
    void listObjects(String bucketName);
    void putObject(String bucketName, String destPath, Path sourcePath);
    ResponseInputStream getObject(String bucketName, String objectKey);
    void deleteObject(String bucketName, String objectKey);
    void deleteObjects(String bucketName, String[] objectKeys);
}
