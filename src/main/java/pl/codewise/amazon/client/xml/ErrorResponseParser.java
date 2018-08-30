package pl.codewise.amazon.client.xml;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.ErrorTagHandler;

import java.io.IOException;
import java.nio.charset.Charset;

public class ErrorResponseParser extends GenericResponseParser<AmazonS3ExceptionBuilder> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorResponseParser.class);

    public ErrorResponseParser(XmlPullParserFactory pullParserFactory) {
        super(pullParserFactory, ErrorTagHandler.UNKNOWN, ErrorTagHandler.values());
    }

    private AmazonS3ExceptionBuilder parseResponse(HttpResponseStatus status, ByteBuf content) throws IOException {
        AmazonS3ExceptionBuilder exceptionBuilder = new AmazonS3ExceptionBuilder();
        exceptionBuilder.setStatusCode(status.code());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Error content body:\n{}", content.toString(Charset.defaultCharset()));
        }

        parse(new ByteBufInputStream(content), exceptionBuilder);
        return exceptionBuilder;
    }

    public AmazonS3ExceptionBuilder parse(HttpResponseStatus status, ByteBuf content) throws IOException {
        try {
            return parseResponse(status, content);
        } finally {
            ReferenceCountUtil.release(content);
        }
    }
}
