package pl.codewise.amazon.client.xml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

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
        return Optional.of(new ByteBufInputStream(content) {

            @Override
            public void close() throws IOException {
                super.close();
                ReferenceCountUtil.release(content);
            }
        });
    }
}
