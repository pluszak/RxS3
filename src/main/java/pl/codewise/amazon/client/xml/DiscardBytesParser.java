package pl.codewise.amazon.client.xml;

import com.ning.http.client.Response;

import java.io.IOException;
import java.util.Optional;

public class DiscardBytesParser extends GenericResponseParser<Object> {

	private static final DiscardBytesParser INSTANCE = new DiscardBytesParser();

	public static DiscardBytesParser getInstance() {
		return INSTANCE;
	}

	public DiscardBytesParser() {
		super(null, null);
	}

	@Override
	public Optional<Object> parse(Response response) throws IOException {
		return Optional.empty();
	}
}
