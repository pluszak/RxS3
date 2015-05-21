package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentialsProvider;

public class ClientConfiguration {

    private AWSCredentialsProvider credentialsProvider;
    private String s3Location;

    private boolean skipParsingOwner;
    private boolean skipParsingETag;
    private boolean skipParsingLastModified;
    private boolean skipParsingStorageClass;

    public ClientConfiguration(AWSCredentialsProvider credentialsProvider, String s3Location, boolean skipParsingOwner, boolean skipParsingETag, boolean skipParsingLastModified, boolean skipParsingStorageClass) {
        this.credentialsProvider = credentialsProvider;
        this.s3Location = s3Location;
        this.skipParsingOwner = skipParsingOwner;
        this.skipParsingETag = skipParsingETag;
        this.skipParsingLastModified = skipParsingLastModified;
        this.skipParsingStorageClass = skipParsingStorageClass;
    }

    public AWSCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public String getS3Location() {
        return s3Location;
    }

    public boolean isSkipParsingOwner() {
        return skipParsingOwner;
    }

    public boolean isSkipParsingETag() {
        return skipParsingETag;
    }

    public boolean isSkipParsingLastModified() {
        return skipParsingLastModified;
    }

    public boolean isSkipParsingStorageClass() {
        return skipParsingStorageClass;
    }

    public static ClientConfigurationBuilder builder() {
        return new ClientConfigurationBuilder();
    }
}
