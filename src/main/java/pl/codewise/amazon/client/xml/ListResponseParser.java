package pl.codewise.amazon.client.xml;

import com.amazonaws.services.s3.model.ObjectListing;
import com.ning.http.client.Response;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.ListObjectsTagHandler;

import java.io.IOException;
import java.util.Optional;

public class ListResponseParser extends GenericResponseParser<ObjectListing> {

	public ListResponseParser(XmlPullParserFactory pullParserFactory) {
		super(pullParserFactory, ListObjectsTagHandler.UNKNOWN, ListObjectsTagHandler.values());
	}

	public Optional<ObjectListing> parse(Response response) throws IOException {
		ObjectListing listing = new ObjectListing();
		parse(response.getResponseBodyAsStream(), listing);

		if (!listing.isTruncated()) {
			listing.setNextMarker(null);
		}

		return Optional.of(listing);
	}
}
