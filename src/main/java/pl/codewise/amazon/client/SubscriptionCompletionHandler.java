package pl.codewise.amazon.client;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import pl.codewise.amazon.client.xml.ErrorResponseParser;
import pl.codewise.amazon.client.xml.GenericResponseParser;
import rx.Observer;
import rx.Subscriber;
import rx.exceptions.OnErrorNotImplementedException;

import static org.slf4j.LoggerFactory.getLogger;

public class SubscriptionCompletionHandler<T> implements Observer<FullHttpResponse> {

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
    public void onNext(FullHttpResponse response) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Amazon response '{}'", response.content().toString(Charset.defaultCharset()));
        }

        if (subscriber.isUnsubscribed()) {
            return;
        }

        if (!emitExceptionIfUnsuccessful(response.status(), response.content(), subscriber)) {
            try {
                Optional<T> result = responseParser.parse(response.status(), response.content());
                if (result.isPresent()) {
                    subscriber.onNext(result.get());
                }

                subscriber.onCompleted();
            } catch (Exception e) {
                subscriber.onError(e);
            }
        }
    }

    @Override
    public void onCompleted() {
    }

    @Override
    public void onError(Throwable t) {
        LOGGER.error("Error while processing S3 request", t);
        subscriber.onError(t);
    }

    private boolean emitExceptionIfUnsuccessful(HttpResponseStatus status, ByteBuf content, Observer<?> observer) {
        try {
            if (!status.equals(HttpResponseStatus.OK) && !status.equals(HttpResponseStatus.NO_CONTENT)) {
                try {
                    observer.onError(errorResponseParser.parse(status, content).get().build());
                } catch (IOException e) {
                    observer.onError(new RuntimeException("Received unparseable error with code: " + status));
                }

                return true;
            }
        } catch (OnErrorNotImplementedException e) {
            LOGGER.error("Error processing response (and onError not implemented)", e);
            observer.onCompleted();
            return true;
        }

        return false;
    }
}
