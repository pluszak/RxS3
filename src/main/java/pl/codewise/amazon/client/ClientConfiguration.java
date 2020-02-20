package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentialsProvider;

public class ClientConfiguration {

    private final AWSCredentialsProvider credentialsProvider;
    private final String s3Location;

    private final int connectionTimeoutMillis;
    private final int requestTimeoutMillis;

    private final int workerThreadCount;

    private final int maxConnections;
    private final int maxPendingAcquires;
    private final int acquireTimeoutMillis;

    private final boolean skipParsingOwner;
    private final boolean skipParsingETag;
    private final boolean skipParsingLastModified;
    private final boolean skipParsingStorageClass;
    private final int maxRetries;

    public ClientConfiguration(
            AWSCredentialsProvider credentialsProvider,
            String s3Location,
            int connectionTimeoutMillis,
            int requestTimeoutMillis,
            int workerThreadCount,
            int maxConnections,
            int maxPendingAcquires,
            int acquireTimeoutMillis,
            boolean skipParsingOwner,
            boolean skipParsingETag,
            boolean skipParsingLastModified,
            boolean skipParsingStorageClass,
            int maxRetries) {
        this.credentialsProvider = credentialsProvider;
        this.s3Location = s3Location;

        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.workerThreadCount = workerThreadCount;

        this.maxConnections = maxConnections;
        this.maxPendingAcquires = maxPendingAcquires;
        this.acquireTimeoutMillis = acquireTimeoutMillis;

        this.skipParsingOwner = skipParsingOwner;
        this.skipParsingETag = skipParsingETag;
        this.skipParsingLastModified = skipParsingLastModified;
        this.skipParsingStorageClass = skipParsingStorageClass;

        this.maxRetries = maxRetries;
    }

    public AWSCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public String getS3Location() {
        return s3Location;
    }

    public int getConnectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public int getWorkerThreadCount() {
        return workerThreadCount;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getMaxPendingAcquires() {
        return maxPendingAcquires;
    }

    public int getAcquireTimeoutMillis() {
        return acquireTimeoutMillis;
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

    public int getMaxRetries() {
        return maxRetries;
    }

    public static ClientConfigurationBuilder builder() {
        return new ClientConfigurationBuilder();
    }
}
