package pl.codewise.amazon.client.xml;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ning.http.client.Response;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.TagHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

public abstract class GenericResponseParser<Context> {

	public static final String INPUT_ENCODING = "UTF-8";

	private XmlPullParserFactory pullParserFactory;

	private final TagHandler<Context> unknownTagHandler;
	private Map<String, TagHandler<Context>> tagHandlerMap;

	@SafeVarargs
	public GenericResponseParser(XmlPullParserFactory pullParserFactory, TagHandler<Context> unknownTagHandler, TagHandler<Context>... tagHandlers) {
		this.pullParserFactory = pullParserFactory;
		this.unknownTagHandler = unknownTagHandler;

		tagHandlerMap = Maps.newHashMap();
		for (TagHandler<Context> tagHandler : tagHandlers) {
			tagHandlerMap.put(tagHandler.getTagName(), tagHandler);
		}
	}

	protected void parse(InputStream responseBodyAsStream, Context context) throws IOException {
		try {
			XmlPullParser parser = pullParserFactory.newPullParser();
			parser.setInput(responseBodyAsStream, INPUT_ENCODING);

			processContents(parser, context);
		} catch (XmlPullParserException e) {
			throw new IOException(e);
		}
	}

	public abstract Optional<Context> parse(Response response) throws IOException;

	private void processContents(XmlPullParser parser, Context exceptionBuilder) throws XmlPullParserException, IOException {
		LinkedList<TagHandler<Context>> handlerStack = Lists.newLinkedList();

		int eventType = parser.getEventType();
		do {
			if (eventType == XmlPullParser.START_TAG) {
				String tagName = parser.getName();
				TagHandler<Context> tagHandler = tagHandlerMap.get(tagName);
				if (tagHandler == null) {
					tagHandler = unknownTagHandler;
				}
				handlerStack.add(tagHandler);

				tagHandler.handleStart(exceptionBuilder, parser);
			} else if (eventType == XmlPullParser.END_TAG) {
				TagHandler<Context> tagHandler = handlerStack.remove(handlerStack.size() - 1);
				tagHandler.handleEnd(exceptionBuilder, parser);
			} else if (eventType == XmlPullParser.TEXT) {
				TagHandler<Context> tagHandler = handlerStack.get(handlerStack.size() - 1);
				tagHandler.handleText(exceptionBuilder, parser, handlerStack);
			}
			eventType = parser.next();
		} while (eventType != XmlPullParser.END_DOCUMENT);
	}
}
