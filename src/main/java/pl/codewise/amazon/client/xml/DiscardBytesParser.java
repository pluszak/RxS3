package pl.codewise.amazon.client.xml;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

public class DiscardBytesParser extends GenericResponseParser<Object> {

    private static final DiscardBytesParser INSTANCE = new DiscardBytesParser();

    public static DiscardBytesParser getInstance() {
        return INSTANCE;
    }

    public DiscardBytesParser() {
        super(null, null);
    }

    @Override
    public Object parse(HttpResponseStatus status, HttpHeaders headers, ByteBuf content) {
        ReferenceCountUtil.release(content);
        return this;
    }
}
