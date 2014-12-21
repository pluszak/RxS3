package pl.codewise.amazon.client.xml;

import javolution.text.CharArray;
import javolution.text.Cursor;
import org.testng.annotations.Test;

import java.util.Calendar;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

public class DateTimeParserTest {

	@Test
	public void shouldParseDate() {
		// Given
		DateTimeParser parser = new DateTimeParser();
		String expected = "2014-12-20T23:02:11.000Z";

		CharArray charArray = new CharArray();
		charArray.setArray(expected.toCharArray(), 0, expected.length());

		// When
		Date actual = parser.parse(charArray, new Cursor(), Calendar.getInstance());

		// Then
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldParseDateWithMillis() {
		// Given
		DateTimeParser parser = new DateTimeParser();
		String expected = "2014-12-20T23:02:11.123Z";

		CharArray charArray = new CharArray();
		charArray.setArray(expected.toCharArray(), 0, expected.length());

		// When
		Date actual = parser.parse(charArray, new Cursor(), Calendar.getInstance());

		// Then
		assertThat(actual).isEqualTo(expected);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void shouldComplainOnInvalidYear() {
		// Given
		DateTimeParser parser = new DateTimeParser();
		String expected = "120a-12-20T23:02:11.123Z";

		CharArray charArray = new CharArray();
		charArray.setArray(expected.toCharArray(), 0, expected.length());

		// When
		parser.parse(charArray, new Cursor(), Calendar.getInstance());
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void shouldComplainOnInvalidYearMonthSeparator() {
		// Given
		DateTimeParser parser = new DateTimeParser();
		String expected = "2120+12-20T23:02:11.123Z";

		CharArray charArray = new CharArray();
		charArray.setArray(expected.toCharArray(), 0, expected.length());

		// When
		parser.parse(charArray, new Cursor(), Calendar.getInstance());
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void shouldComplainOnInvalidMonthDaysSeparator() {
		// Given
		DateTimeParser parser = new DateTimeParser();
		String expected = "2120-12+20T23:02:11.123Z";

		CharArray charArray = new CharArray();
		charArray.setArray(expected.toCharArray(), 0, expected.length());

		// When
		parser.parse(charArray, new Cursor(), Calendar.getInstance());
	}
}
