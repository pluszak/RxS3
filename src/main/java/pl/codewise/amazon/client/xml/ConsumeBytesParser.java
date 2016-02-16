package pl.codewise.amazon.client.xml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ConsumeBytesParser extends GenericResponseParser<InputStream> {

    private static final ConsumeBytesParser INSTANCE = new ConsumeBytesParser();

    public static ConsumeBytesParser getInstance() {
        return INSTANCE;
    }

    private ConsumeBytesParser() {
        super(null, null);
    }

    @Override
    public Optional<InputStream> parse(HttpResponseStatus status, ByteBuf content) throws IOException {
        //TODO looks like leak here, I think we should return ByteBuf to release later or copy content and release here
        return Optional.of(new ByteBufInputStream(content));
    }
}
