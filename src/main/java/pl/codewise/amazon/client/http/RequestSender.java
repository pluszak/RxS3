package pl.codewise.amazon.client.http;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;

public class RequestSender implements FutureListener<Channel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyHttpClient.class);

    private final Request requestData;
    private final SubscriptionCompletionHandler completionHandler;

    public RequestSender(Request requestData, SubscriptionCompletionHandler completionHandler) {
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
        if (requestData.getMethod().equals(HttpMethod.PUT)) {
            request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, requestData.getMethod(), requestData.getUrl(), requestData.getBody());
        } else {
            request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, requestData.getMethod(), requestData.getUrl());
        }

        request.headers().set(HttpHeaderNames.HOST, requestData.getBucketName() + ".s3.amazonaws.com");
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        request.headers().set(HttpHeaderNames.CONTENT_TYPE, requestData.getContentType());
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, requestData.getContentLength());
        request.headers().set(HttpHeaderNames.CONTENT_MD5, requestData.getMd5());

        requestData.getSignatureCalculator().calculateAndAddSignature(request.headers(),
                requestData.getUrl(), requestData.getMd5(), requestData.getContentType(), requestData.getBucketName());

        channel.writeAndFlush(request);
    }
}
