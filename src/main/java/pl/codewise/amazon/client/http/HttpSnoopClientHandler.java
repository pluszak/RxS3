package pl.codewise.amazon.client.http;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.apache.commons.lang3.tuple.Pair;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;

public class HttpSnoopClientHandler<T> extends SimpleChannelInboundHandler<HttpObject> {

    private final SubscriptionCompletionHandler<T> consumer;

    private CompositeByteBuf result = Unpooled.compositeBuffer();
    private HttpResponseStatus status;

    public HttpSnoopClientHandler(SubscriptionCompletionHandler<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            status = response.getStatus();
        }

        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;

            content.retain();
            result.addComponent(content.content());
            result.writerIndex(result.writerIndex() + content.content().readableBytes());
            if (content instanceof LastHttpContent) {
                ctx.close();
                consumer.onNext(Pair.of(status, result));
                consumer.onCompleted();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
