package pl.codewise.amazon.client.xml;

import com.google.common.collect.Maps;
import com.ning.http.client.Response;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.TagHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

public abstract class GenericResponseParser<Context> {

	public static final String INPUT_ENCODING = "UTF-8";

	private XmlPullParserFactory pullParserFactory;

	private final TagHandler<Context> unknownTagHandler;
	private final Map<String, TagHandler<Context>> tagHandlerMap;

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

	private void processContents(XmlPullParser parser, Context context) throws XmlPullParserException, IOException {
		ContextStack<Context> handlerStack = ContextStack.<Context>getInstance();

		int eventType = parser.getEventType();
		do {
			if (eventType == XmlPullParser.START_TAG) {
				String tagName = parser.getName();
				TagHandler<Context> tagHandler = tagHandlerMap.getOrDefault(tagName, unknownTagHandler);
				handlerStack.push(tagHandler).handleStart(context, parser);
			} else if (eventType == XmlPullParser.END_TAG) {
				handlerStack.pop().handleEnd(context, parser);
			} else if (eventType == XmlPullParser.TEXT) {
				handlerStack.top().handleText(context, parser, handlerStack);
			}
			eventType = parser.next();
		} while (eventType != XmlPullParser.END_DOCUMENT);
	}
}
