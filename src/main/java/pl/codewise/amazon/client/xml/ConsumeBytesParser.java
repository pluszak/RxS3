package pl.codewise.amazon.client.xml;

import java.io.IOException;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import pl.codewise.amazon.client.SizedInputStream;

public class ConsumeBytesParser extends GenericResponseParser<SizedInputStream> {

    private static final ConsumeBytesParser INSTANCE = new ConsumeBytesParser();

    public static ConsumeBytesParser getInstance() {
        return INSTANCE;
    }

    private ConsumeBytesParser() {
        super(null, null);
    }

    @Override
    public Optional<SizedInputStream> parse(HttpResponseStatus status, ByteBuf content) throws IOException {
        return Optional.of(new SizedInputStream(content));
    }
}
