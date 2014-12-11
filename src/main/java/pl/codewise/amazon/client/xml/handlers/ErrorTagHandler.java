package pl.codewise.amazon.client.xml.handlers;

import org.xmlpull.v1.XmlPullParser;
import pl.codewise.amazon.client.xml.AmazonS3ExceptionBuilder;
import pl.codewise.amazon.client.xml.ContextStack;

public enum ErrorTagHandler implements TagHandler<AmazonS3ExceptionBuilder> {

	ERROR("Error") {
	}, CODE("Code") {
		@Override
		public void handleText(AmazonS3ExceptionBuilder exception, XmlPullParser parser, ContextStack handlerStack) {
			exception.setErrorCode(parser.getText());
		}
	}, MESSAGE("Message") {
		@Override
		public void handleText(AmazonS3ExceptionBuilder exception, XmlPullParser parser, ContextStack handlerStack) {
			exception.setMessage(parser.getText());
		}
	}, RESOURCE("Resource") {
		@Override
		public void handleText(AmazonS3ExceptionBuilder exception, XmlPullParser parser, ContextStack handlerStack) {

		}
	}, REQUEST_ID("RequestId") {
		@Override
		public void handleText(AmazonS3ExceptionBuilder exception, XmlPullParser parser, ContextStack handlerStack) {
			exception.setRequestId(parser.getText());
		}
	},
	UNKNOWN("unknown");

	private String tagName;

	ErrorTagHandler(String tagName) {
		this.tagName = tagName;
	}

	@Override
	public String getTagName() {
		return tagName;
	}

	public void handleText(AmazonS3ExceptionBuilder objectListing, XmlPullParser parser, ContextStack handlerStack) {
	}

	public void handleStart(AmazonS3ExceptionBuilder objectListing, XmlPullParser parser) {
	}

	public void handleEnd(AmazonS3ExceptionBuilder objectListing, XmlPullParser parser) {
	}
}
