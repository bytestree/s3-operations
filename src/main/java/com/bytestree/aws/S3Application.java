package com.bytestree.aws;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Scanner;

/**
 * Program to connect to AWS S3 and perform GET/PUT/DELETE/LIST
 * operations using accesskey or IAM role
 *
 * @author bytestree
 * @see <a href="http://www.bytestree.com/">BytesTree</a>
 */
public class S3Application {
    private static final Logger logger = LogManager.getLogger(S3Application.class);

    public static void main(String[] args) throws IOException {

        // Get bucket name and region
        logger.info("Enter bucket name to connect:");
        Scanner in = new Scanner(System.in);
        String bucketName = in.nextLine();
        logger.info("Enter bucket Region: ");
        String region = in.nextLine();

        // get S3Client for bucket operations
        S3Client s3client = getS3Client(region);

        // Initialize S3Service
        S3Service s3Service = new S3ServiceImpl(s3client);

        String filePath = "";
        logger.info("Enter operation to perform on bucket: {} . LIST/GET/PUT/DELETE/DELETE-MULTIPLE", bucketName);
        String operation = in.nextLine();


        if (operation.equalsIgnoreCase("LIST")) {
            s3Service.listObjects(bucketName);
            logger.info("Listing completed");
        }
        else if(operation.equalsIgnoreCase("GET")) {
            logger.info("Enter full path of object in bucket:");
            filePath = in.nextLine();
            ResponseInputStream inputStream = s3Service.getObject(bucketName, filePath);
            // Create file from inputStream to downloads directory in current location
            FileUtils.copyInputStreamToFile(inputStream, new File("downloads/"+ filePath));
            IOUtils.closeQuietly(inputStream, null);
            logger.info("Download completed");
        }
        else if(operation.equalsIgnoreCase("PUT")) {
            logger.info("Enter local full path of file to upload:");
            String inputPath = in.nextLine();
            logger.info("Enter full path in bucket:");
            String destPath = in.nextLine();
            Path inPath = FileSystems.getDefault().getPath(inputPath);
            s3Service.putObject(bucketName, destPath, inPath);
            logger.info("Put Operation Completed");
        }
        else if(operation.equalsIgnoreCase("DELETE")) {
            logger.info("Enter full path of object to DELETE in bucket:");
            filePath = in.nextLine();
            s3Service.deleteObject(bucketName, filePath);
            logger.info("Delete Operation Completed");
        }
        else if(operation.equalsIgnoreCase("DELETE-MULTIPLE")) {
            logger.info("Enter comma separated full path of object to DELETE in bucket:");
            filePath = in.nextLine();
            String filePaths[] = filePath.split(",");
            s3Service.deleteObjects(bucketName, filePaths);
            logger.info("Delete Operation Completed");
        }
        else {
            logger.error("Invalid Operation");
        }
        logger.info("Exiting...");
    }

    private static S3Client getS3Client(String region) {
        Scanner in = new Scanner(System.in);
        S3Client s3client = null;

        logger.info("Connect S3 via IAM role? y/n");
        String useIAM = in.nextLine();

        if(useIAM.equalsIgnoreCase("y")) {
            s3client = buildS3Client(Region.of(region));
        } else if(useIAM.equalsIgnoreCase("n")){
            logger.info("Enter Access Key: ");
            String accessKey = in.nextLine();
            logger.info("Enter Secret Key: ");
            String secretKey = in.nextLine();
            s3client = buildS3Client(accessKey, secretKey, Region.of(region));
        }
        return s3client;
    }

    private static S3Client buildS3Client(String accessKey, String secretKey, Region region) {
        StaticCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider
                                        .create(AwsBasicCredentials.create(accessKey, secretKey));
        S3Client s3client = S3Client.builder().
                            credentialsProvider(staticCredentialsProvider)
                            .region(region).build();
        return s3client;
    }

    private static S3Client buildS3Client(Region region) {
        S3Client s3client = S3Client.builder().
                            credentialsProvider(InstanceProfileCredentialsProvider.create())
                            .region(region).build();
        return s3client;
    }
}
