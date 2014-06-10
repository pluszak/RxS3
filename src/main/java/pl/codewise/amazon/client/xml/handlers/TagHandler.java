package pl.codewise.amazon.client.xml.handlers;

import org.xmlpull.v1.XmlPullParser;

import java.util.LinkedList;

public interface TagHandler<Context> {

	String getTagName();

	void handleText(Context context, XmlPullParser parser, LinkedList<? extends  TagHandler<Context>> handlerStack);

	void handleStart(Context context, XmlPullParser parser);

	void handleEnd(Context context, XmlPullParser parser);
}
