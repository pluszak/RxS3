package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentials;

public class ClientConfigurationBuilder {

	private AWSCredentials credentials;
	private String s3Location;

	private boolean skipParsingOwner;
	private boolean skipParsingETag;
	private boolean skipParsingLastModified;
	private boolean skipParsingStorageClass;

	public ClientConfigurationBuilder useCredentials(AWSCredentials credentials) {
		this.credentials = credentials;
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
		return new ClientConfiguration(credentials, s3Location, skipParsingOwner, skipParsingETag, skipParsingLastModified, skipParsingStorageClass);
	}
}
