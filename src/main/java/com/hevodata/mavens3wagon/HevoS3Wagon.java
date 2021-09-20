package com.hevodata.mavens3wagon;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import kong.unirest.GetRequest;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONException;
import kong.unirest.json.JSONObject;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.repository.Repository;
import org.kuali.maven.wagon.S3Wagon;
import org.kuali.maven.wagon.auth.AwsSessionCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HevoS3Wagon extends S3Wagon {

    private String authUrl;
    private static final Logger log = LoggerFactory.getLogger(HevoS3Wagon.class);

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
            GetRequest getRequest = Unirest.get(this.getAuthUrl());
            if (authenticationInfo != null && authenticationInfo.getUserName() != null) {
                getRequest.basicAuth(authenticationInfo.getUserName(), authenticationInfo.getPassword());
            }
            HttpResponse<JsonNode> response = getRequest.asJson();
            JSONObject jsonObject = response.getBody().getObject();
            String accessKey = jsonObject.getString("access_key");
            String secretKey = jsonObject.getString("secret_key");
            String sessionToken = jsonObject.getString("token");
            return new AwsSessionCredentials(accessKey, secretKey, sessionToken);
        } catch (JSONException e) {
            log.error(e.getMessage());
            throw e;
        }
    }
}
