package pl.codewise.amazon.client.auth;

import javolution.text.TextBuilder;

public enum Operation {
    GET,
    PUT {
        @Override
        public void getResourceName(TextBuilder builder, CharSequence objectName) {
            builder.append(objectName);
        }
    },
    LIST("GET") {
        @Override
        public void getResourceName(TextBuilder builder, CharSequence objectName) {
            builder.append('/');
        }
    },
    DELETE,
    BULK_DELETE("POST") {
        @Override
        public void getResourceName(TextBuilder builder, CharSequence objectName) {
            super.getResourceName(builder, objectName);
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

    public void getResourceName(TextBuilder builder, CharSequence objectName) {
        builder.append('/');
        builder.append(objectName, 1, objectName.length());
    }

    public String getOperationName() {
        return operationName;
    }
}
