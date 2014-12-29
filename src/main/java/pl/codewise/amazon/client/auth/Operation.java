package pl.codewise.amazon.client.auth;

import com.ning.http.client.Request;
import javolution.text.TextBuilder;

public enum Operation {
	GET,
	PUT {
		@Override
		public void getResourceName(TextBuilder builder, Request request) {
			String objectName = request.getUri().getPath();
			builder.append(objectName);
		}
	},
	LIST("GET") {
		@Override
		public void getResourceName(TextBuilder builder, Request request) {
			builder.append('/');
		}
	},
	DELETE,
	BULK_DELETE("POST") {
		@Override
		public void getResourceName(TextBuilder builder, Request request) {
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

	public void getResourceName(TextBuilder builder, Request request) {
		builder.append('/');

		String objectName = request.getUri().getPath();
		builder.append(objectName, 1, objectName.length());
	}

	public String getOperationName() {
		return operationName;
	}
}
