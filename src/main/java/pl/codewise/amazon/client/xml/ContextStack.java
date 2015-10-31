package pl.codewise.amazon.client.xml;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javolution.text.CharArray;
import javolution.text.Cursor;
import org.xmlpull.v1.XmlPullParser;
import pl.codewise.amazon.client.xml.handlers.TagHandler;

public class ContextStack<Context> {

    private static final ThreadLocal<ContextStack> INSTANCE = new ThreadLocal<ContextStack>() {
        @Override
        protected ContextStack initialValue() {
            return new ContextStack();
        }
    };

    private final List<TagHandler<Context>> handlerStack = new ArrayList<>(3);

    private final int[] parseResultStartAndLength = new int[2];
    private final CharArray parseResultHolder = new CharArray();
    private final Cursor cursor = new Cursor();
    private final Calendar calendar = Calendar.getInstance();

    public TagHandler<Context> push(TagHandler<Context> handler) {
        handlerStack.add(handler);
        return handler;
    }

    public TagHandler<Context> pop() {
        return handlerStack.remove(handlerStack.size() - 1);
    }

    public TagHandler<Context> top() {
        return handlerStack.get(handlerStack.size() - 1);
    }

    public TagHandler<Context> topMinusOne() {
        return handlerStack.get(handlerStack.size() - 2);
    }

    private ContextStack<Context> cleared() {
        handlerStack.clear();
        return this;
    }

    public CharArray getTextCharacters(XmlPullParser parser) {
        char[] characters = parser.getTextCharacters(parseResultStartAndLength);
        return parseResultHolder.setArray(characters, parseResultStartAndLength[0], parseResultStartAndLength[1]);
    }

    public Cursor getCursor() {
        cursor.setIndex(0);
        return cursor;
    }

    public Calendar getCalendar() {
        return calendar;
    }

    @SuppressWarnings("unchecked")
    public static <Context> ContextStack<Context> getInstance() {
        return INSTANCE.get().cleared();
    }
}
