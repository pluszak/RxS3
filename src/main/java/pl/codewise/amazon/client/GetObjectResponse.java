package pl.codewise.amazon.client;

import com.amazonaws.services.s3.Headers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpHeaders;

public class GetObjectResponse {

    private final ByteBufInputStream content;

    private final int contentLength;
    private final HttpHeaders headers;

    public GetObjectResponse(ByteBuf content, HttpHeaders headers) {
        this.content = new ByteBufInputStream(content, true);
        this.contentLength = content.readableBytes();
        this.headers = headers;
    }

    public ByteBufInputStream getContent() {
        return content;
    }

    public int getContentLength() {
        return contentLength;
    }

    public String getETag() {
        return headers.get(Headers.ETAG);
    }

    public HttpHeaders getHeaders() {
        return headers;
    }
}
