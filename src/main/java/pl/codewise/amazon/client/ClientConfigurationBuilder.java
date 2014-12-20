package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentials;

public class ClientConfigurationBuilder {

	private AWSCredentials credentials;

	private boolean skipParsingOwner;
	private boolean skipParsingETag;
	private boolean skipParsingLastModified;
	private boolean skipParsingStorageClass;

	public ClientConfigurationBuilder useCredentials(AWSCredentials credentials) {
		this.credentials = credentials;
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
		return new ClientConfiguration(credentials, skipParsingOwner, skipParsingETag, skipParsingLastModified, skipParsingStorageClass);
	}
}
