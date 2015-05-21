package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;

public class ClientConfigurationBuilder {

    public static final String DEFAULT_S3_LOCATION = "s3.amazonaws.com";

    private String s3Location = DEFAULT_S3_LOCATION;
    private AWSCredentialsProvider credentialsProvider;

    private boolean skipParsingOwner;
    private boolean skipParsingETag;
    private boolean skipParsingLastModified;
    private boolean skipParsingStorageClass;

    public ClientConfigurationBuilder useCredentials(AWSCredentials credentials) {
        this.credentialsProvider = new StaticCredentialsProvider(credentials);

        return this;
    }

    public ClientConfigurationBuilder withCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
        return this;

    }

    public ClientConfigurationBuilder connectTo(String s3Location) {
        this.s3Location = s3Location;
        return this;
    }

    public ClientConfigurationBuilder skipParsingOwner() {
        skipParsingOwner = true;
        return this;
    }

    public ClientConfigurationBuilder skipParsingETag() {
        skipParsingETag = true;
        return this;
    }

    public ClientConfigurationBuilder skipParsingLasModified() {
        skipParsingLastModified = true;
        return this;
    }

    public ClientConfigurationBuilder skipParsingStorageClass() {
        skipParsingStorageClass = true;
        return this;
    }

    public ClientConfiguration build() {
        return new ClientConfiguration(credentialsProvider, s3Location, skipParsingOwner, skipParsingETag, skipParsingLastModified, skipParsingStorageClass);
    }
}
