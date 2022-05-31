package com.bytestree.aws;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Service implementation for S3 operations
 *
 * @author bytestree
 */
public class S3ServiceImpl implements S3Service {
    private final S3Client s3client;
    private static final Logger logger = LogManager.getLogger(S3ServiceImpl.class);

    //constructor
    public S3ServiceImpl(S3Client s3client) {
        this.s3client = s3client;
    }

    /**
     * List all objects in bucket
     * @param bucketName
     */
    public void listObjects(String bucketName) {
        ListObjectsV2Iterable listObjectsV2Iterable = s3client.listObjectsV2Paginator(builder -> builder.bucket(bucketName));
        logger.info("Objects in {} bucket: ", bucketName);
        listObjectsV2Iterable.contents().stream()
                .forEach(content -> logger.debug("{} {} bytes", content.key(), content.size()));
    }

    /**
     * Upload object to bucket
     * @param bucketName
     * @param destPath
     * @param sourcePath
     */
    public void putObject(String bucketName, String destPath, Path sourcePath) {
        s3client.putObject(builder -> builder.bucket(bucketName).key(destPath), sourcePath);
    }

    /**
     * Get the inputstream of object in bucket to download
     * @param bucketName
     * @param objectKey
     * @return
     */
    public ResponseInputStream getObject(String bucketName, String objectKey) {
        return s3client.getObject(builder -> builder.bucket(bucketName).key(objectKey));
    }

    /**
     * Delete single object from bucket
     * @param bucketName
     * @param objectKey
     */
    public void deleteObject(String bucketName, String objectKey) {
        s3client.deleteObject(builder -> builder.bucket(bucketName).key(objectKey));
    }

    /**
     * Delete Multiple objects from bucket
     * @param bucketName
     * @param keys
     */
    public void deleteObjects(String bucketName, String[] keys) {
        // Create ObjectIdentifier from keys to delete
        List<ObjectIdentifier> objectIdentifiers = Stream.of(keys)
                                                    .map(key -> ObjectIdentifier.builder().key(key.trim()).build())
                                                    .collect(Collectors.toList());
        // Create DeleteObjectsRequest using objectIdentifiers
        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                                                    .bucket(bucketName)
                                                    .delete(deleteBuilder -> deleteBuilder.objects(objectIdentifiers))
                                                    .build();

        s3client.deleteObjects(deleteObjectsRequest);
    }
}
