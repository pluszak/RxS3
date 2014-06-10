package pl.codewise.amazon.client.xml;

import com.amazonaws.services.s3.model.AmazonS3Exception;

public class AmazonS3ExceptionBuilder {

	public static final String SERVICE_NAME = "Amazon S3";

	private String message;
	private int statusCode;

	private String errorCode;
	private String resource;
	private String requestId;

	public void setMessage(String message) {
		this.message = message;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	@SuppressWarnings("UnusedDeclaration")
	public void setResource(String resource) {
		this.resource = resource;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public AmazonS3Exception build() {
		AmazonS3Exception result = new AmazonS3Exception(message);

		result.setStatusCode(statusCode);
		result.setErrorCode(errorCode);
		result.setRequestId(requestId);
		result.setServiceName(SERVICE_NAME);

		return result;
	}
}
