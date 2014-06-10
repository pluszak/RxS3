package pl.codewise.amazon.client.xml;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.ErrorTagHandler;

import java.io.IOException;
import java.io.InputStream;

public class ErrorResponseParser extends GenericResponseParser<AmazonS3ExceptionBuilder> {

	public ErrorResponseParser(XmlPullParserFactory pullParserFactory) {
		super(pullParserFactory, ErrorTagHandler.UNKNOWN, ErrorTagHandler.values());
	}

	public AmazonS3Exception parse(InputStream responseBodyAsStream, int statusCode) throws IOException {
		AmazonS3ExceptionBuilder exceptionBuilder = new AmazonS3ExceptionBuilder();
		exceptionBuilder.setStatusCode(statusCode);

		parse(responseBodyAsStream, exceptionBuilder);
		return exceptionBuilder.build();
	}
}
