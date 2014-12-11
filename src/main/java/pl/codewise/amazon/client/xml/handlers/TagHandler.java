package pl.codewise.amazon.client.xml.handlers;

import org.xmlpull.v1.XmlPullParser;
import pl.codewise.amazon.client.xml.ContextStack;

public interface TagHandler<Context> {

	String getTagName();

	void handleText(Context context, XmlPullParser parser, ContextStack handlerStack);

	void handleStart(Context context, XmlPullParser parser);

	void handleEnd(Context context, XmlPullParser parser);
}
