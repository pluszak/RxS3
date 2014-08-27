package pl.codewise.amazon.client.auth;

import com.ning.http.client.Request;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public enum Operation {
	GET, PUT {
		@Override
		public String getResourceName(String bucketName, Request request) {
			String objectName = request.getURI().getPath();
			return "/" + bucketName + objectName;
		}
	}, LIST("GET"), DELETE, BULK_DELETE("POST") {
		@Override
		public String getResourceName(String bucketName, Request request) {
			return super.getResourceName(bucketName, request) + "?delete";
		}
	};

	private String operationName;

	Operation() {
		operationName = name();
	}

	Operation(String operationName) {
		this.operationName = operationName;
	}

	public String getResourceName(String bucketName, Request request) {
		if (this == LIST) {
			return "/" + bucketName + "/";
		}

		String objectName = request.getURI().getPath();
		objectName = objectName.substring(1);
		try {
			objectName = "/" + URLEncoder.encode(objectName, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}

		return "/" + bucketName + objectName;
	}

	public String getOperationName() {
		return operationName;
	}
}
