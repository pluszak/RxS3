package pl.codewise.amazon.client;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.FlowableEmitter;
import pl.codewise.amazon.client.xml.ErrorResponseParser;
import pl.codewise.amazon.client.xml.GenericResponseParser;

public class SubscriptionCompletionHandler<T> {

    private AtomicBoolean downstreamNotified = new AtomicBoolean();
    private final FlowableEmitter<? super T> subscriber;

    private final GenericResponseParser<T> responseParser;
    private final ErrorResponseParser errorResponseParser;

    SubscriptionCompletionHandler(FlowableEmitter<? super T> subscriber, GenericResponseParser<T> responseParser, ErrorResponseParser errorResponseParser) {
        this.subscriber = subscriber;

        this.responseParser = responseParser;
        this.errorResponseParser = errorResponseParser;
    }

    public void onSuccess(FullHttpResponse response) {
        if (subscriber.isCancelled() || !downstreamNotified.compareAndSet(false, true)) {
            ReferenceCountUtil.release(response);
            return;
        }

        if (!emitExceptionIfUnsuccessful(response.getStatus(), response.content(), subscriber)) {
            try {
                Optional<T> result = responseParser.parse(response.getStatus(), response.content());
                if (result.isPresent()) {
                    subscriber.onNext(result.get());
                }

                subscriber.onComplete();
            } catch (Exception e) {
                subscriber.onError(e);
            }
        }
    }

    public void onError(Throwable t) {
        if (downstreamNotified.compareAndSet(false, true)) {
            subscriber.onError(t);
        }
    }

    private boolean emitExceptionIfUnsuccessful(HttpResponseStatus status, ByteBuf content, FlowableEmitter<?> observer) {
        if (!status.equals(HttpResponseStatus.OK) && !status.equals(HttpResponseStatus.NO_CONTENT)) {
            try {
                observer.onError(errorResponseParser.parse(status, content).get().build());
            } catch (IOException e) {
                observer.onError(new RuntimeException("Received unparseable error with code: " + status));
            }

            return true;
        }

        return false;
    }
}
