package pl.codewise.amazon.client;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.slf4j.Logger;
import pl.codewise.amazon.client.xml.ErrorResponseParser;
import pl.codewise.amazon.client.xml.GenericResponseParser;
import rx.Observer;
import rx.Subscriber;

import java.io.IOException;
import java.util.Optional;

import static org.slf4j.LoggerFactory.getLogger;

class SubscriptionCompletionHandler<T> extends AsyncCompletionHandler<T> {

	private static final Logger LOGGER = getLogger(SubscriptionCompletionHandler.class);

	private final Subscriber<? super T> subscriber;

	private final GenericResponseParser<T> responseParser;
	private final ErrorResponseParser errorResponseParser;

	public SubscriptionCompletionHandler(Subscriber<? super T> subscriber, GenericResponseParser<T> responseParser, ErrorResponseParser errorResponseParser) {
		this.subscriber = subscriber;

		this.responseParser = responseParser;
		this.errorResponseParser = errorResponseParser;
	}

	@Override
	public T onCompleted(Response response) throws IOException {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Amazon response '{}'", response.getResponseBody());
		}

		if (subscriber.isUnsubscribed()) {
			return ignoreReturnValue();
		}

		if (!emitExceptionIfUnsuccessful(response, subscriber)) {
			try {
				Optional<T> result = responseParser.parse(response);
				if (result.isPresent()) {
					subscriber.onNext(result.get());
				}

				subscriber.onCompleted();
			} catch (Exception e) {
				subscriber.onError(e);
			}
		}

		return ignoreReturnValue();
	}

	@Override
	public void onThrowable(Throwable t) {
		LOGGER.error("Error while processing S3 request", t);
		subscriber.onError(t);
	}

	private boolean emitExceptionIfUnsuccessful(Response response, Observer<?> observer) throws IOException {
		if (response.getStatusCode() != 200 && response.getStatusCode() != 204) {
			observer.onError(errorResponseParser.parse(response).get().build());
			return true;
		}

		return false;
	}

	T ignoreReturnValue() {
		return null;
	}
}
