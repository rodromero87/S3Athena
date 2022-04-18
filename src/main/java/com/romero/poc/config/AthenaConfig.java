package com.romero.poc.config;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

@Configuration
public class AthenaConfig {

    @Value("${aws.accessKeyId}")
    private String accessKey;
    @Value("${aws.secretKey}")
    private String secretKey;

    @Bean
    public AthenaClient createAthenaClient(){
        return AthenaClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(getCredentials()).build();
    }

    private AwsCredentialsProvider getCredentials() {

        return new AwsCredentialsProvider() {
            @Override
            public AwsCredentials resolveCredentials() {
                return new Credentials(accessKey, secretKey);
            }
        };
    }

}

 class Credentials implements AwsCredentials{

    private final String accessKey;
    private final String secretKey;

    public Credentials(String accessKey, String secretKey) {
        if (accessKey == null) {
            throw new IllegalArgumentException("Access key cannot be null.");
        } else if (secretKey == null) {
            throw new IllegalArgumentException("Secret key cannot be null.");
        } else {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }
    }

     @Override
     public String accessKeyId() {
         return accessKey;
     }

     @Override
     public String secretAccessKey() {
         return secretKey;
     }
 }
