package com.hevodata.mavens3wagon;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kong.unirest.GetRequest;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.repository.Repository;
import org.kuali.maven.wagon.S3Wagon;
import org.kuali.maven.wagon.auth.AwsSessionCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

public class HevoS3Wagon extends S3Wagon {

    private String authUrl;
    private static final Logger log = LoggerFactory.getLogger(HevoS3Wagon.class);
    private final File credentialsCachePath = new File("/tmp/hevo_s3_wagon");

    public void setAuthUrl(String authUrl) {
        this.authUrl = authUrl;
    }

    public String getAuthUrl() {
        return this.authUrl;
    }

    @Override
    protected CannedAccessControlList getAclFromRepository(Repository repository) {
        return CannedAccessControlList.Private;
    }

    @Override
    protected AWSCredentials getCredentials(final AuthenticationInfo authenticationInfo) {
        try {
            if(!credentialsCachePath.exists()){
                getAwsSessionCredentials(authenticationInfo);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> credentials = objectMapper.readValue(credentialsCachePath, new TypeReference<Map<String,Object>>(){});
            long expiry = ((Number) credentials.getOrDefault("expiry", 0)).longValue();
            if ((System.currentTimeMillis() / 1000) > expiry) {
                getAwsSessionCredentials(authenticationInfo);
                credentials = objectMapper.readValue(credentialsCachePath, new TypeReference<Map<String,Object>>(){});
            }
            String accessKey = (String) credentials.get("access_key");
            String secretKey = (String) credentials.get("secret_key");
            String sessionToken = (String) credentials.get("token");
            return new AwsSessionCredentials(accessKey, secretKey, sessionToken);
        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        }
    }

    private void getAwsSessionCredentials(final AuthenticationInfo authenticationInfo) throws FileNotFoundException {
        GetRequest getRequest = Unirest.get(this.getAuthUrl());
        if (authenticationInfo != null && authenticationInfo.getUserName() != null) {
            getRequest.basicAuth(authenticationInfo.getUserName(), authenticationInfo.getPassword());
        }

        HttpResponse<JsonNode> response = getRequest.asJson();
        try (PrintWriter out = new PrintWriter(credentialsCachePath)) {
            out.println(response.getBody().getObject().toString());
        }
    }
}
