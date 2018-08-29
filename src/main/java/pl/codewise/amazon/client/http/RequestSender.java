package pl.codewise.amazon.client.http;

import com.netflix.concurrency.limits.Limiter;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;
import pl.codewise.amazon.client.auth.Operation;

class RequestSender implements FutureListener<Channel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestSender.class);

    private final String s3Location;
    private final HandlerDemultiplexer demultiplexer;
    private final ChannelPool channelPool;

    private final Request requestData;
    private final SubscriptionCompletionHandler completionHandler;
    private final Limiter.Listener token;

    RequestSender(String s3Location, Request requestData, SubscriptionCompletionHandler completionHandler, HandlerDemultiplexer demultiplexer, ChannelPool channelPool, Limiter.Listener token) {
        this.s3Location = s3Location;
        this.requestData = requestData;
        this.completionHandler = completionHandler;
        this.demultiplexer = demultiplexer;
        this.channelPool = channelPool;
        this.token = token;
    }

    @Override
    public void operationComplete(Future<Channel> future) {
        if (!future.isSuccess()) {
            token.onIgnore();
            completionHandler.onError(future.cause());
        } else {
            try {
                executeRequest(future.getNow(), requestData);
            } catch (Exception e) {
                token.onIgnore();
                completionHandler.onError(e);
            }
        }
    }

    private void executeRequest(Channel channel, Request requestData) {
        DefaultFullHttpRequest request;
        if (requestData.getOperation().equals(Operation.PUT)) {
            request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, requestData.getOperation().getHttpMethod(), requestData.getUrl(), requestData.getBody());
        } else {
            request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, requestData.getOperation().getHttpMethod(), requestData.getUrl());
        }

        request.headers().set(HttpHeaders.Names.HOST, requestData.getBucketName() + "." + s3Location);
        request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);

        request.headers().set(HttpHeaders.Names.CONTENT_TYPE, requestData.getContentType());
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, requestData.getContentLength());
        request.headers().set(HttpHeaders.Names.CONTENT_MD5, requestData.getMd5());

        requestData.getSignatureCalculatorFactory().getSignatureCalculator()
                .calculateAndAddSignature(request.headers(), requestData);

        HttpClientHandler httpClientHandler = new HttpClientHandler(channelPool, completionHandler, token);
        demultiplexer.setAttributeValue(channel, httpClientHandler);
        channel.writeAndFlush(request)
                .addListener(writeFuture -> {
                    if (!writeFuture.isSuccess()) {
                        LOGGER.error("Exception during write and flush", writeFuture.cause());
                        httpClientHandler.exceptionCaught(channel, writeFuture.cause());
                    }
                });
    }
}
