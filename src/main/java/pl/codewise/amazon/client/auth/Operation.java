package pl.codewise.amazon.client.auth;

import com.ning.http.client.Request;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public enum Operation {
	GET,
	PUT {
		@Override
		public void getResourceName(StringBuilder builder, Request request) {
			String objectName = request.getURI().getPath();
			builder.append(objectName);
		}
	},
	LIST("GET") {
		@Override
		public void getResourceName(StringBuilder builder, Request request) {
			builder.append('/');
		}
	},
	DELETE,
	BULK_DELETE("POST") {
		@Override
		public void getResourceName(StringBuilder builder, Request request) {
			super.getResourceName(builder, request);
			builder.append("?delete");
		}
	};

	private String operationName;

	Operation() {
		operationName = name();
	}

	Operation(String operationName) {
		this.operationName = operationName;
	}

	public void getResourceName(StringBuilder builder, Request request) {
		String objectName = request.getURI().getPath();
		objectName = objectName.substring(1);
		try {
			objectName = "/" + URLEncoder.encode(objectName, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}

		builder.append(objectName);
	}

	public String getOperationName() {
		return operationName;
	}
}
