package pl.codewise.amazon.client.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import pl.codewise.amazon.client.auth.AWSSignatureCalculator;

public class Request {

    private String url;
    private HttpMethod method;

    private String bucketName;
    private AWSSignatureCalculator signatureCalculator;

    private byte[] body;

    private String contentType = "";
    private long contentLength;
    private String md5 = "";

    public Request(String url, HttpMethod method) {
        this.url = url;
        this.method = method;
    }

    public Request setBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public Request setSignatureCalculator(AWSSignatureCalculator signatureCalculator) {
        this.signatureCalculator = signatureCalculator;
        return this;
    }

    public Request setBody(byte[] body) {
        this.body = body;
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

    public HttpMethod getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    }

    public String getBucketName() {
        return bucketName;
    }

    public AWSSignatureCalculator getSignatureCalculator() {
        return signatureCalculator;
    }

    public ByteBuf getBody() {
        return Unpooled.wrappedBuffer(body);
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
