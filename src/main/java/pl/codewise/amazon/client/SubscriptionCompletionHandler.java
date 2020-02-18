package pl.codewise.amazon.client;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.SingleEmitter;
import org.slf4j.Logger;
import pl.codewise.amazon.client.http.Request;
import pl.codewise.amazon.client.xml.ErrorResponseParser;
import pl.codewise.amazon.client.xml.GenericResponseParser;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.slf4j.LoggerFactory.getLogger;

public class SubscriptionCompletionHandler<T> {

    private static final Logger LOGGER = getLogger(SubscriptionCompletionHandler.class);

    private final AtomicBoolean downstreamNotified = new AtomicBoolean();
    private final SingleEmitter<? super T> subscriber;

    private final Request request;
    private final GenericResponseParser<T> responseParser;
    private final ErrorResponseParser errorResponseParser;

    SubscriptionCompletionHandler(SingleEmitter<? super T> subscriber, Request request, GenericResponseParser<T> responseParser, ErrorResponseParser errorResponseParser) {
        this.subscriber = subscriber;
        this.request = request;

        this.responseParser = responseParser;
        this.errorResponseParser = errorResponseParser;
    }

    public void onSuccess(FullHttpResponse response) {
        if (subscriber.isDisposed() || !downstreamNotified.compareAndSet(false, true)) {
            ReferenceCountUtil.release(response);
            return;
        }

        if (!emitExceptionIfUnsuccessful(response, subscriber)) {
            try {
                T result = responseParser.parse(
                        response.getStatus(),
                        response.headers(),
                        response.content()
                );
                subscriber.onSuccess(result);
            } catch (Exception e) {
                subscriber.onError(e);
            }
        }
    }

    public void onError(Throwable t) {
        if (downstreamNotified.compareAndSet(false, true)) {
            if (subscriber.isDisposed()) {
                LOGGER.error("Failed reqeust: {}", request.getUrl());
            } else {
                subscriber.onError(t);
            }
        }
    }

    private boolean emitExceptionIfUnsuccessful(FullHttpResponse response, SingleEmitter<?> observer) {
        HttpResponseStatus status = response.getStatus();
        if (!status.equals(HttpResponseStatus.OK) && !status.equals(HttpResponseStatus.NO_CONTENT)) {
            try {
                observer.onError(
                        errorResponseParser.parse(
                                status,
                                response.headers(),
                                response.content()
                        ).build()
                );
            } catch (IOException e) {
                observer.onError(new RuntimeException("Received unparseable error with code: " + status));
            }

            return true;
        }

        return false;
    }

    void cancel() {
        if (!downstreamNotified.get()) {
            LOGGER.error("Cancelled request {}", request.getUrl());
        }
    }
}
