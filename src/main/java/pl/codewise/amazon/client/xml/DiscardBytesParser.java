package pl.codewise.amazon.client.xml;

import java.io.IOException;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;

public class DiscardBytesParser extends GenericResponseParser<Object> {

	private static final DiscardBytesParser INSTANCE = new DiscardBytesParser();

	public static DiscardBytesParser getInstance() {
		return INSTANCE;
	}

	public DiscardBytesParser() {
		super(null, null);
	}

	@Override
	public Optional<Object> parse(HttpResponseStatus status, ByteBuf content) throws IOException {
		return Optional.empty();
	}
}
