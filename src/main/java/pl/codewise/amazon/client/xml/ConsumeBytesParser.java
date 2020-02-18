package pl.codewise.amazon.client.xml;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import pl.codewise.amazon.client.GetObjectResponse;

public class ConsumeBytesParser extends GenericResponseParser<GetObjectResponse> {

    private static final ConsumeBytesParser INSTANCE = new ConsumeBytesParser();

    public static ConsumeBytesParser getInstance() {
        return INSTANCE;
    }

    private ConsumeBytesParser() {
        super(null, null);
    }

    @Override
    public GetObjectResponse parse(HttpResponseStatus status,
                                   HttpHeaders headers,
                                   ByteBuf content) {
        return new GetObjectResponse(content, headers);
    }
}
