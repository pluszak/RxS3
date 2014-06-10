package pl.codewise.amazon.client.auth;

import com.ning.http.client.Request;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public enum Operation {
	GET, LIST("GET"), DELETE, BULK_DELETE("POST") {
		@Override
		public String getResourceName(Request request) {
			return super.getResourceName(request) + "?delete";
		}
	};

	private String operationName;

	Operation() {
		operationName = name();
	}

	Operation(String operationName) {
		this.operationName = operationName;
	}

	public String getResourceName(Request request) {
//		String URL_PATH_OTHER_SAFE_CHARS =
//				"-._*,/";
//
//
//		PercentEscaper percentEscaper = new PercentEscaper(URL_PATH_OTHER_SAFE_CHARS, false);
		String virtualHost = request.getVirtualHost();
		String bucketName = virtualHost.substring(0, virtualHost.indexOf('.'));

		if (this == LIST) {
			return "/" + bucketName + "/";
		}

//		String objectName = request.getURI().getPath();
//		objectName = objectName.substring(1);
//		objectName = "/" + percentEscaper.escape(objectName);
//
//		return "/" + bucketName + objectName;

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
