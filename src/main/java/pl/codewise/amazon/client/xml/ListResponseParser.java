package pl.codewise.amazon.client.xml;

import com.amazonaws.services.s3.model.ObjectListing;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.ListObjectsTagHandler;

import java.io.IOException;
import java.io.InputStream;

public class ListResponseParser extends GenericResponseParser<ObjectListing> {

	public ListResponseParser(XmlPullParserFactory pullParserFactory) {
		super(pullParserFactory, ListObjectsTagHandler.UNKNOWN,  ListObjectsTagHandler.values());
	}

	public ObjectListing parse(InputStream responseBodyAsStream) throws IOException {
		ObjectListing listing = new ObjectListing();
		parse(responseBodyAsStream, listing);

		return listing;
	}
}
