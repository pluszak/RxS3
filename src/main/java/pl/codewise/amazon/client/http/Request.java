package pl.codewise.amazon.client.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pl.codewise.amazon.client.auth.AWSSignatureCalculatorFactory;
import pl.codewise.amazon.client.auth.Operation;

public class Request {

    private String url;
    private Operation operation;

    private String bucketName;
    private AWSSignatureCalculatorFactory signatureCalculatorFactory;

    private ByteBuf body;

    private String contentType = "";
    private long contentLength;
    private String md5 = "";

    public Request(String url, Operation operation) {
        this.url = url;
        this.operation = operation;
    }

    public Request setBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public Request setSignatureCalculatorFactory(AWSSignatureCalculatorFactory signatureCalculatorFactory) {
        this.signatureCalculatorFactory = signatureCalculatorFactory;
        return this;
    }

    public Request setBody(ByteBuf body) {
        this.body = Unpooled.unreleasableBuffer(body);
        return this;
    }

    public Request setContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public Request setContentLength(long contentLength) {
        this.contentLength = contentLength;
        return this;
    }

    public Request setMd5(String md5) {
        this.md5 = md5;
        return this;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getUrl() {
        return url;
    }

    public String getBucketName() {
        return bucketName;
    }

    public AWSSignatureCalculatorFactory getSignatureCalculatorFactory() {
        return signatureCalculatorFactory;
    }

    public ByteBuf getBody() {
        return body;
    }

    public String getContentType() {
        return contentType;
    }

    public long getContentLength() {
        return contentLength;
    }

    public String getMd5() {
        return md5;
    }

    public Request build() {
        return this;
    }
}
