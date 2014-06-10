package pl.codewise.amazon.client.auth;

import com.amazonaws.auth.AWSCredentials;

public class AWSSignatureCalculatorAggregate {

	private final AWSSignatureCalculator getSignatureCalculator;
	private final AWSSignatureCalculator listSignatureCalculator;
	private final AWSSignatureCalculator deleteSignatureCalculator;
	private final AWSSignatureCalculator bulkDeleteSignatureCalculator;

	public AWSSignatureCalculatorAggregate(AWSCredentials credentials) {
		getSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.GET);
		listSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.LIST);
		deleteSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.DELETE);
		bulkDeleteSignatureCalculator = new AWSSignatureCalculator(credentials, Operation.BULK_DELETE);
	}

	public AWSSignatureCalculator getGetSignatureCalculator() {
		return getSignatureCalculator;
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
