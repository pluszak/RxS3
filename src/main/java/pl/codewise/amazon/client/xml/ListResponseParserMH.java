package pl.codewise.amazon.client.xml;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Maps;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;

public class ListResponseParserMH {

	public static final String INPUT_ENCODING = "UTF-8";

	private Map<String, MethodHandle> headerHandlers;

	private XmlPullParserFactory pullParserFactory;

	public ListResponseParserMH(XmlPullParserFactory pullParserFactory) {
		this.pullParserFactory = pullParserFactory;

		headerHandlers = Maps.newHashMap();

		try {
			MethodHandle stringFilter = MethodHandles.lookup().findStatic(ListResponseParserMH.class, "parseString", MethodType.methodType(String.class, XmlPullParser.class));
			MethodHandle booleanFilter = MethodHandles.lookup().findStatic(ListResponseParserMH.class, "parseBoolean", MethodType.methodType(boolean.class, XmlPullParser.class));
			MethodHandle intFilter = MethodHandles.lookup().findStatic(ListResponseParserMH.class, "parseInteger", MethodType.methodType(int.class, XmlPullParser.class));

			MethodHandle handle = MethodHandles.lookup().findVirtual(ObjectListing.class, "setTruncated", MethodType.methodType(void.class, boolean.class));
			headerHandlers.put("IsTruncated", MethodHandles.filterArguments(handle, 1, booleanFilter));

			handle = MethodHandles.lookup().findVirtual(ObjectListing.class, "setBucketName", MethodType.methodType(void.class, String.class));
			headerHandlers.put("Name", MethodHandles.filterArguments(handle, 1, stringFilter));

			handle = MethodHandles.lookup().findVirtual(ObjectListing.class, "setPrefix", MethodType.methodType(void.class, String.class));
			headerHandlers.put("Prefix", MethodHandles.filterArguments(handle, 1, stringFilter));

			handle = MethodHandles.lookup().findVirtual(ObjectListing.class, "setMaxKeys", MethodType.methodType(void.class, int.class));
			headerHandlers.put("MaxKeys", MethodHandles.filterArguments(handle, 1, intFilter));

			handle = MethodHandles.lookup().findVirtual(ObjectListing.class, "setKey", MethodType.methodType(void.class, String.class));
			headerHandlers.put("Key", MethodHandles.filterArguments(handle, 1, stringFilter));

			handle = MethodHandles.lookup().findVirtual(ObjectListing.class, "setKey", MethodType.methodType(void.class, String.class));
			headerHandlers.put("Contents.Key", MethodHandles.filterArguments(handle, 1, stringFilter));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ObjectListing parse(InputStream responseBodyAsStream) throws XmlPullParserException, IOException {
		XmlPullParser parser = pullParserFactory.newPullParser();
		parser.setInput(responseBodyAsStream, INPUT_ENCODING);

		ObjectListing listing = new ObjectListing();
		processContents(parser, listing);

		return listing;
	}

	private void processContents(XmlPullParser parser, ObjectListing listing) throws XmlPullParserException, IOException {
		S3ObjectSummary summary = null;

		int eventType = parser.getEventType();
		MethodHandle handle = null;
		do {
			if (eventType == XmlPullParser.START_TAG) {
				String tagName = parser.getName();
				handle = headerHandlers.get(tagName);

				if (handle == null) {
					if(tagName.equals("Contents")) {
						summary = new S3ObjectSummary();
					}
				}
			} else if (eventType == XmlPullParser.END_TAG) {
				handle = null;
			} else if (eventType == XmlPullParser.TEXT) {
				if (handle == null) {
					throw new IllegalStateException();
				}

				try {
					handle.invokeExact(listing, parser);
				} catch (Throwable throwable) {
					throwable.printStackTrace();
				}
			}
			eventType = parser.next();
		} while (eventType != XmlPullParser.END_DOCUMENT);
	}

	public static String parseString(XmlPullParser parser) {
		return parser.getText();
	}

	public static boolean parseBoolean(XmlPullParser parser) {
		return Boolean.parseBoolean(parser.getText());
	}

	public static int parseInteger(XmlPullParser parser) {
		return Integer.parseInt(parser.getText());
	}
}
