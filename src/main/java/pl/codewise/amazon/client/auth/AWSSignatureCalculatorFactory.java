package pl.codewise.amazon.client.auth;

import com.amazonaws.auth.AWSCredentials;

public class AWSSignatureCalculatorFactory {

	private AWSCredentials credentials;

	public AWSSignatureCalculatorFactory(AWSCredentials credentials) {
		this.credentials = credentials;
	}

	public AWSSignatureCalculator getGetSignatureCalculator(String bucketName) {
		return new AWSSignatureCalculator(credentials, bucketName, Operation.GET);
	}

	public AWSSignatureCalculator getPutSignatureCalculator(String bucketName) {
		return new AWSSignatureCalculator(credentials, bucketName, Operation.PUT);
	}

	public AWSSignatureCalculator getListSignatureCalculator(String bucketName) {
		return new AWSSignatureCalculator(credentials, bucketName, Operation.LIST);
	}

	public AWSSignatureCalculator getDeleteSignatureCalculator(String bucketName) {
		return new AWSSignatureCalculator(credentials, bucketName, Operation.DELETE);
	}

	public AWSSignatureCalculator getBulkDeleteSignatureCalculator(String bucketName) {
		return new AWSSignatureCalculator(credentials, bucketName, Operation.BULK_DELETE);
	}
}
