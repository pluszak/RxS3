package pl.codewise.amazon.client.xml;

import com.ning.http.client.Response;

import java.io.IOException;
import java.io.InputStream;

public class PassThroughParser extends GenericResponseParser<InputStream> {

	public PassThroughParser() {
		super(null, null);
	}

	@Override
	public InputStream parse(Response response) throws IOException {
		return response.getResponseBodyAsStream();
	}
}
