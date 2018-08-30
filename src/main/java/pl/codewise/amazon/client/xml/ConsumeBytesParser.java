package pl.codewise.amazon.client.xml;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import pl.codewise.amazon.client.SizedInputStream;

import java.io.IOException;

public class ConsumeBytesParser extends GenericResponseParser<SizedInputStream> {

    private static final ConsumeBytesParser INSTANCE = new ConsumeBytesParser();

    public static ConsumeBytesParser getInstance() {
        return INSTANCE;
    }

    private ConsumeBytesParser() {
        super(null, null);
    }

    @Override
    public SizedInputStream parse(HttpResponseStatus status, ByteBuf content) throws IOException {
        return new SizedInputStream(content);
    }
}
