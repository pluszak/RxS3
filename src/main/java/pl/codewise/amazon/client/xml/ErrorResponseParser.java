package pl.codewise.amazon.client.xml;

import com.ning.http.client.Response;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.ErrorTagHandler;

import java.io.IOException;

public class ErrorResponseParser extends GenericResponseParser<AmazonS3ExceptionBuilder> {

	public ErrorResponseParser(XmlPullParserFactory pullParserFactory) {
		super(pullParserFactory, ErrorTagHandler.UNKNOWN, ErrorTagHandler.values());
	}

	public AmazonS3ExceptionBuilder parse(Response response) throws IOException {
		AmazonS3ExceptionBuilder exceptionBuilder = new AmazonS3ExceptionBuilder();
		exceptionBuilder.setStatusCode(response.getStatusCode());

		parse(response.getResponseBodyAsStream(), exceptionBuilder);
		return exceptionBuilder;
	}
}
