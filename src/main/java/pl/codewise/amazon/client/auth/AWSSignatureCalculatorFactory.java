package pl.codewise.amazon.client.auth;

import com.amazonaws.auth.AWSCredentials;

public class AWSSignatureCalculatorFactory {

	private final AWSSignatureCalculator getSignatureCalculator;
	private final AWSSignatureCalculator putSignatureCalculator;
	private final AWSSignatureCalculator listSignatureCalculator;
	private final AWSSignatureCalculator deleteSignatureCalculator;
	private final AWSSignatureCalculator bulkDeleteSignatureCalculator;

	public AWSSignatureCalculatorFactory(AWSCredentials credentials, String s3Location) {
		getSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.GET, s3Location);
		putSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.PUT, s3Location);
		listSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.LIST, s3Location);
		deleteSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.DELETE, s3Location);
		bulkDeleteSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.BULK_DELETE, s3Location);
	}

	public AWSSignatureCalculator getGetSignatureCalculator() {
		return getSignatureCalculator;
	}

	public AWSSignatureCalculator getPutSignatureCalculator() {
		return putSignatureCalculator;
	}

	public AWSSignatureCalculator getListSignatureCalculator() {
		return listSignatureCalculator;
	}

	public AWSSignatureCalculator getDeleteSignatureCalculator() {
		return deleteSignatureCalculator;
	}

	public AWSSignatureCalculator getBulkDeleteSignatureCalculator() {
		return bulkDeleteSignatureCalculator;
	}
}
