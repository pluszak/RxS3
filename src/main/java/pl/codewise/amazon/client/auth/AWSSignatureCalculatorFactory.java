package pl.codewise.amazon.client.auth;

import com.amazonaws.auth.AWSCredentials;

public class AWSSignatureCalculatorFactory {

	private final AWSSignatureCalculator getSignatureCalculator;
	private final AWSSignatureCalculator putSignatureCalculator;
	private final AWSSignatureCalculator listSignatureCalculator;
	private final AWSSignatureCalculator deleteSignatureCalculator;
	private final AWSSignatureCalculator bulkDeleteSignatureCalculator;

	public AWSSignatureCalculatorFactory(AWSCredentials credentials) {
		getSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.GET);
		putSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.PUT);
		listSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.LIST);
		deleteSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.DELETE);
		bulkDeleteSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.BULK_DELETE);
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
