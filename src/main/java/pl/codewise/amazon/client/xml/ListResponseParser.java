package pl.codewise.amazon.client.xml;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.amazonaws.services.s3.model.ObjectListing;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.ClientConfiguration;
import pl.codewise.amazon.client.xml.handlers.ListObjectsTagHandler;
import pl.codewise.amazon.client.xml.handlers.TagHandler;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

public class ListResponseParser extends GenericResponseParser<ObjectListing> {

    public ListResponseParser(XmlPullParserFactory pullParserFactory, Map<String, TagHandler<ObjectListing>> tagHandlerMap) {
        super(pullParserFactory, ListObjectsTagHandler.UNKNOWN, tagHandlerMap);
    }

    public Optional<ObjectListing> parse(HttpResponseStatus status, ByteBuf content) throws IOException {
        ObjectListing listing = new ObjectListing();
        parse(new ByteBufInputStream(content), listing);

        if (!listing.isTruncated()) {
            listing.setNextMarker(null);
        }

        return Optional.of(listing);
    }

    public static ListResponseParser newListResponseParser(XmlPullParserFactory pullParserFactory, ClientConfiguration configuration) {
        EnumSet<ListObjectsTagHandler> excludedHandlers = EnumSet.noneOf(ListObjectsTagHandler.class);
        if (configuration.isSkipParsingStorageClass()) {
            excludedHandlers.add(ListObjectsTagHandler.STORAGE_CLASS);
        }

        if (configuration.isSkipParsingLastModified()) {
            excludedHandlers.add(ListObjectsTagHandler.LAST_MODIFIED);
        }

        if (configuration.isSkipParsingETag()) {
            excludedHandlers.add(ListObjectsTagHandler.ETAG);
        }

        if (configuration.isSkipParsingOwner()) {
            excludedHandlers.add(ListObjectsTagHandler.OWNER);
            excludedHandlers.add(ListObjectsTagHandler.ID);
            excludedHandlers.add(ListObjectsTagHandler.DISPLAY_NAME);
        }

        return new ListResponseParser(pullParserFactory, stream(ListObjectsTagHandler.values())
                .filter((handler) -> !excludedHandlers.contains(handler))
                .collect(toMap(TagHandler::getTagName, Function.<TagHandler<ObjectListing>>identity())));
    }
}
