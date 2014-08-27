package pl.codewise.amazon.client.xml;

import com.ning.http.client.Response;

import java.io.IOException;
import java.util.Optional;

public class ConsumeBytesParser extends GenericResponseParser<byte[]> {

	private static final ConsumeBytesParser INSTANCE = new ConsumeBytesParser();

	public static ConsumeBytesParser getInstance() {
		return INSTANCE;
	}

	private ConsumeBytesParser() {
		super(null, null);
	}

	@Override
	public Optional<byte[]> parse(Response response) throws IOException {
		return Optional.of(response.getResponseBodyAsBytes());
	}
}
