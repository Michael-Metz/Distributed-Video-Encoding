import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class S3Repository {

    private final static Regions REGION = Regions.US_EAST_2;
    private ProfileCredentialsProvider credentials;
    private AmazonS3 s3Client;
    private String bucketName;

    public S3Repository(String buckerName) {
        this.bucketName = buckerName;
        credentials = new ProfileCredentialsProvider();
        try {
            s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(REGION)
                    .withCredentials(credentials)
                    .build();
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        }
    }

    public void putFile(File file, String fileName) {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            putFileInputStream(fileInputStream, fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void putFileInputStream(FileInputStream fileInputStream, String filename) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType("video/mp4");
        PutObjectRequest request = new PutObjectRequest(bucketName, filename, fileInputStream, objectMetadata);
        request.setCannedAcl(CannedAccessControlList.PublicRead);
        s3Client.putObject(request);
    }

    public void downloadFile(String remoteFileName, String newFilePath) {
        S3Object s3object = s3Client.getObject(bucketName, remoteFileName);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        try {
            FileUtils.copyInputStreamToFile(inputStream, new File(newFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
