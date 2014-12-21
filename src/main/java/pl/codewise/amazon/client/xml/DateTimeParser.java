package pl.codewise.amazon.client.xml;

import javolution.text.CharArray;
import javolution.text.Cursor;
import javolution.text.TypeFormat;

import java.util.Calendar;
import java.util.Date;

public class DateTimeParser {

	public Date parse(CharArray text, Cursor cursor, Calendar calendar) {
		cursor.setIndex(0);
		calendar.set(Calendar.YEAR, TypeFormat.parseInt(text, cursor));

		verifyCharacterAndAdvanceCursor(text, cursor, '-');
		calendar.set(Calendar.MONTH, TypeFormat.parseInt(text, cursor) - 1);

		verifyCharacterAndAdvanceCursor(text, cursor, '-');
		calendar.set(Calendar.DAY_OF_MONTH, TypeFormat.parseInt(text, cursor));

		verifyCharacterAndAdvanceCursor(text, cursor, 'T');
		calendar.set(Calendar.HOUR_OF_DAY, TypeFormat.parseInt(text, cursor));

		verifyCharacterAndAdvanceCursor(text, cursor, ':');
		calendar.set(Calendar.MINUTE, TypeFormat.parseInt(text, cursor));

		verifyCharacterAndAdvanceCursor(text, cursor, ':');
		calendar.set(Calendar.SECOND, TypeFormat.parseInt(text, cursor));

		verifyCharacterAndAdvanceCursor(text, cursor, '.');
		calendar.set(Calendar.MILLISECOND, TypeFormat.parseInt(text, cursor));

		verifyCharacterAndAdvanceCursor(text, cursor, 'Z');
		return calendar.getTime();
	}

	private void verifyCharacterAndAdvanceCursor(CharArray text, Cursor cursor, char expectedCharacter) {
		if (text.charAt(cursor.getIndex()) != expectedCharacter) {
			throw new IllegalArgumentException(text.toString());
		}

		cursor.increment();
	}
}
