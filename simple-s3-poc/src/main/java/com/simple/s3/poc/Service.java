package com.simple.s3.poc;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@org.springframework.stereotype.Service
public class Service {

	@Autowired
	private AWSs3FileUtility awsS3FileUtility;

	@Value("${s3.read.bucket}")
	private String readBucket;
	
	@Value("${app.server.download.location}")
	private String appServerDownloadLocation;
	
	@Value("${app.server.download.file.name}")
	private String appServerDownloadFileName;
	
	@Value("${s3.read.bucket.input.folder}")
	private String s3BucketInputFolder;
	
	@Value("${s3.read.bucket.output.folder}")
	private String s3BucketOutputFolder;
	
	@Value("${s3.read.bucket.read.files}")
	private int s3BucketReadNoOfFiles;
	
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	public void serve() {
		
		List<String> keys = awsS3FileUtility.listKeys(readBucket,s3BucketInputFolder,s3BucketReadNoOfFiles);
		keys.forEach(sourceKey->{
			
			 LOGGER.info("Precessing key: {}", sourceKey);
			 awsS3FileUtility.downloadFileFromS3(readBucket, sourceKey, appServerDownloadLocation+appServerDownloadFileName);
			 String destinationKey = s3BucketOutputFolder + sourceKey.substring(s3BucketInputFolder.length()); 
			 awsS3FileUtility.copyObject(readBucket, sourceKey, readBucket,destinationKey );
			 awsS3FileUtility.deleteObject(readBucket, sourceKey);
		});
		

	}

}
