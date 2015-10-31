package pl.codewise.amazon.client.auth;

import io.netty.handler.codec.http.HttpMethod;
import javolution.text.TextBuilder;

public enum Operation {
    GET(HttpMethod.GET),
    PUT(HttpMethod.PUT) {
        @Override
        public void getResourceName(TextBuilder builder, CharSequence objectName) {
            builder.append(objectName);
        }
    },
    LIST(HttpMethod.GET) {
        @Override
        public void getResourceName(TextBuilder builder, CharSequence objectName) {
            builder.append('/');
        }
    },
    DELETE(HttpMethod.DELETE),
    BULK_DELETE(HttpMethod.POST) {
        @Override
        public void getResourceName(TextBuilder builder, CharSequence objectName) {
            super.getResourceName(builder, objectName);
            builder.append("?delete");
        }
    };

    private final HttpMethod httpMethod;

    Operation(HttpMethod httpMethod) {
        this.httpMethod = httpMethod;
    }

    public void getResourceName(TextBuilder builder, CharSequence objectName) {
        builder.append('/');
        builder.append(objectName, 1, objectName.length());
    }

    public HttpMethod getHttpMethod() {
        return httpMethod;
    }
}
