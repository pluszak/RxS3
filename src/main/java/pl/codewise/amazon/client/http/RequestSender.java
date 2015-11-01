package pl.codewise.amazon.client.http;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;
import pl.codewise.amazon.client.auth.Operation;

public class RequestSender implements FutureListener<Channel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyHttpClient.class);

    private final String s3Location;
    private final Request requestData;
    private final SubscriptionCompletionHandler completionHandler;

    public RequestSender(String s3Location, Request requestData, SubscriptionCompletionHandler completionHandler) {
        this.s3Location = s3Location;
        this.requestData = requestData;
        this.completionHandler = completionHandler;
    }

    @Override
    public void operationComplete(Future<Channel> future) throws Exception {
        if (!future.isSuccess()) {
            completionHandler.onError(future.cause());
        } else {
            executeRequest(future.getNow(), requestData, completionHandler);
        }
    }

    private <T> void executeRequest(Channel channel, Request requestData, SubscriptionCompletionHandler<T> completionHandler) throws Exception {
        SubscriptionCompletionHandler previous = channel.attr(HttpClientHandler.HANDLER_ATTRIBUTE_KEY).getAndSet(completionHandler);
        if (previous != null) {
            LOGGER.error("Internal error, completion handler should have been null");
        }

        DefaultFullHttpRequest request;
        if (requestData.getOperation().equals(Operation.PUT)) {
            request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, requestData.getOperation().getHttpMethod(), requestData.getUrl(), requestData.getBody());
        } else {
            request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, requestData.getOperation().getHttpMethod(), requestData.getUrl());
        }

        request.headers().set(HttpHeaderNames.HOST, requestData.getBucketName() + "." + s3Location);
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        request.headers().set(HttpHeaderNames.CONTENT_TYPE, requestData.getContentType());
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, requestData.getContentLength());
        request.headers().set(HttpHeaderNames.CONTENT_MD5, requestData.getMd5());

        requestData.getSignatureCalculatorFactory().getSignatureCalculator()
                .calculateAndAddSignature(request.headers(), requestData);

        channel.writeAndFlush(request);
    }
}
