package pl.codewise.amazon.client.xml;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.xml.handlers.TagHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Function;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

public abstract class GenericResponseParser<Context> {

    public static final String INPUT_ENCODING = "UTF-8";

    private XmlPullParserFactory pullParserFactory;

    private final TagHandler<Context> unknownTagHandler;
    private final Map<String, TagHandler<Context>> tagHandlerMap;

    public GenericResponseParser(XmlPullParserFactory pullParserFactory, TagHandler<Context> unknownTagHandler, Map<String, TagHandler<Context>> tagHandlerMap) {
        this.pullParserFactory = pullParserFactory;
        this.unknownTagHandler = unknownTagHandler;
        this.tagHandlerMap = tagHandlerMap;
    }

    @SafeVarargs
    public GenericResponseParser(XmlPullParserFactory pullParserFactory, TagHandler<Context> unknownTagHandler, TagHandler<Context>... tagHandlers) {
        this(pullParserFactory, unknownTagHandler, stream(tagHandlers).collect(toMap(TagHandler::getTagName, Function.<TagHandler<Context>>identity())));
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

    public abstract Context parse(HttpResponseStatus status, ByteBuf content) throws IOException;

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
