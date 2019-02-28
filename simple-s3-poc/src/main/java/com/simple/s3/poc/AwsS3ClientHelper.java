package com.simple.s3.poc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

@Component
public class AwsS3ClientHelper {


    public static AmazonS3 amazonS3 = null;

    @Value("${s3.accessKey}")
    private String ACCESS_KEY;

    @Value("${s3.accessSecret}")
    private String ACCESS_SECRET;

    public AmazonS3 getAmazonS3Instance() {

        if(amazonS3 == null) {
            AWSStaticCredentialsProvider credentialProviderChain = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(ACCESS_KEY, ACCESS_SECRET));
            amazonS3 = AmazonS3ClientBuilder.standard().withCredentials(credentialProviderChain).withRegion(Regions.AP_SOUTH_1).build();
        }

        return amazonS3;
    }

}

