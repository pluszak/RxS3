package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentials;

public class ClientConfiguration {

	private AWSCredentials credentials;
	private String s3Location;

	private boolean skipParsingOwner;
	private boolean skipParsingETag;
	private boolean skipParsingLastModified;
	private boolean skipParsingStorageClass;

	public ClientConfiguration(AWSCredentials credentials, String s3Location, boolean skipParsingOwner, boolean skipParsingETag, boolean skipParsingLastModified, boolean skipParsingStorageClass) {
		this.credentials = credentials;
		this.s3Location = s3Location;
		this.skipParsingOwner = skipParsingOwner;
		this.skipParsingETag = skipParsingETag;
		this.skipParsingLastModified = skipParsingLastModified;
		this.skipParsingStorageClass = skipParsingStorageClass;
	}

	public AWSCredentials getCredentials() {
		return credentials;
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
