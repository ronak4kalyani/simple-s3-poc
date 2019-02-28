package com.simple.s3.poc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

@Service
public class AWSs3FileUtility {

	@Autowired
	private AwsS3ClientHelper awsS3ClientHelper;

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	public void uploadFileToS3(String bucket, String key, File file) {
		LOGGER.info("saving file to s3 " + key);
		
		AmazonS3 s3Client = awsS3ClientHelper.getAmazonS3Instance();

		if (file != null) {
			try {
				s3Client.putObject(new PutObjectRequest(bucket, key, file));
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}

	}

	public void downloadFileFromS3(String bucket, String key, String destinationFile) {
		LOGGER.info("downloading file from s3");

		AmazonS3 s3Instance = awsS3ClientHelper.getAmazonS3Instance();

		try {

			GetObjectRequest request = new GetObjectRequest(bucket, key);
			S3Object s3Object = s3Instance.getObject(request);

			InputStream reader = new BufferedInputStream(s3Object.getObjectContent());
			File file = new File(destinationFile);
			OutputStream writer = new BufferedOutputStream(new FileOutputStream(file));

			int read = -1;
			while ((read = reader.read()) != -1) {
				writer.write(read);
			}
			writer.flush();
			writer.close();
			reader.close();

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	public List<String> listKeys(String bucketName, String prefix, int noOfFiles) {
		
		LOGGER.info("Listing keys.");
		AmazonS3 s3Client = awsS3ClientHelper.getAmazonS3Instance();
		try {

			ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix).withMaxKeys(noOfFiles+1);
			ListObjectsV2Result result;

			result = s3Client.listObjectsV2(req);
			return result.getObjectSummaries().stream().map(S3ObjectSummary::getKey).filter(key -> !key.equals(prefix))
					.collect(Collectors.toList());

		} catch (AmazonServiceException e) {
			LOGGER.error("The call was transmitted successfully, but Amazon S3 couldn't process it, so it returned an error response.\n" + e.getMessage(), e);
		} catch (SdkClientException e) {
			LOGGER.error("Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response from Amazon S3.\n" + e.getMessage(), e);
		}
		
		return null;
    }
	
	public void copyObject(String sourceBucket, String sourceKey, String destinationBucket, String destinationKey) {
		
		try {
			LOGGER.info("Copy the object having source bucket {} and source key {} into destination {} bucket and destination key {}",
					sourceBucket, sourceKey, destinationBucket, destinationKey);
			AmazonS3 s3Client = awsS3ClientHelper.getAmazonS3Instance();

			CopyObjectRequest copyObjRequest = new CopyObjectRequest(sourceBucket, sourceKey, destinationBucket, destinationKey);
			s3Client.copyObject(copyObjRequest);
		} catch (AmazonServiceException e) {
			LOGGER.error("The call was transmitted successfully, but Amazon S3 couldn't process it, so it returned an error response.\n" + e.getMessage(), e);
		} catch (SdkClientException e) {
			LOGGER.error("Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response from Amazon S3.\n" + e.getMessage(), e);
		}
	}
	
	public void deleteObject(String sourceBucket, String sourceKey) {
		
		try {
			LOGGER.info("Deleting the object having source bucket {} and {}  source key.", sourceBucket, sourceKey);
			AmazonS3 s3Client = awsS3ClientHelper.getAmazonS3Instance();

			DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(sourceBucket, sourceKey);
			s3Client.deleteObject(deleteObjectRequest);
		} catch (AmazonServiceException e) {
			LOGGER.error("The call was transmitted successfully, but Amazon S3 couldn't process it, so it returned an error response.\n" + e.getMessage(), e);
		} catch (SdkClientException e) {
			LOGGER.error("Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response from Amazon S3.\n" + e.getMessage(), e);
		}
	}

}
