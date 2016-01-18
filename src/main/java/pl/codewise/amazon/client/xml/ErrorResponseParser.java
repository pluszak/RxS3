package pl.codewise.amazon.client.xml;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.ErrorTagHandler;

public class ErrorResponseParser extends GenericResponseParser<AmazonS3ExceptionBuilder> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorResponseParser.class);

    public ErrorResponseParser(XmlPullParserFactory pullParserFactory) {
        super(pullParserFactory, ErrorTagHandler.UNKNOWN, ErrorTagHandler.values());
    }

    public AmazonS3ExceptionBuilder parseResponse(HttpResponseStatus status, ByteBuf content) throws IOException {
        AmazonS3ExceptionBuilder exceptionBuilder = new AmazonS3ExceptionBuilder();
        exceptionBuilder.setStatusCode(status.code());

        LOGGER.error("Error content body:\n{}", content.toString(Charset.defaultCharset()));
        parse(new ByteBufInputStream(content), exceptionBuilder);

        return exceptionBuilder;
    }

    public Optional<AmazonS3ExceptionBuilder> parse(HttpResponseStatus status, ByteBuf content) throws IOException {
        AmazonS3ExceptionBuilder response = parseResponse(status, content);
        ReferenceCountUtil.release(content);
        
        return Optional.of(response);
    }
}
