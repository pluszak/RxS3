package pl.codewise.amazon.client.xml;

import com.ning.http.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.ErrorTagHandler;

import java.io.IOException;
import java.util.Optional;

public class ErrorResponseParser extends GenericResponseParser<AmazonS3ExceptionBuilder> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ErrorResponseParser.class);

	public ErrorResponseParser(XmlPullParserFactory pullParserFactory) {
		super(pullParserFactory, ErrorTagHandler.UNKNOWN, ErrorTagHandler.values());
	}

	public AmazonS3ExceptionBuilder parseResponse(Response response) throws IOException {
		AmazonS3ExceptionBuilder exceptionBuilder = new AmazonS3ExceptionBuilder();
		exceptionBuilder.setStatusCode(response.getStatusCode());

		LOGGER.error("Error response body:\n{}", response.getResponseBody());
		parse(response.getResponseBodyAsStream(), exceptionBuilder);

		return exceptionBuilder;
	}

	public Optional<AmazonS3ExceptionBuilder> parse(Response response) throws IOException {
		return Optional.of(parseResponse(response));
	}
}
