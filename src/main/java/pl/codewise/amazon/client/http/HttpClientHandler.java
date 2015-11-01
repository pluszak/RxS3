package pl.codewise.amazon.client.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.codec.http.*;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.tuple.Pair;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;

public class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {

    public static final AttributeKey<SubscriptionCompletionHandler> HANDLER_ATTRIBUTE_KEY = AttributeKey.valueOf("handler");

    private final ChannelPool channelPool;
    private final ByteBuf result = Unpooled.buffer();

    private HttpResponseStatus status;
    private boolean keepAlive;

    public HttpClientHandler(ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            keepAlive = HttpUtil.isKeepAlive(response);
            status = response.status();
        }

        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;

            result.writeBytes(content.content());

            if (content instanceof LastHttpContent) {
                Attribute<SubscriptionCompletionHandler> handlerAttribute = ctx.attr(HANDLER_ATTRIBUTE_KEY);
                SubscriptionCompletionHandler handler = handlerAttribute.getAndRemove();

                if (!keepAlive) {
                    ctx.close();
                }

                channelPool.release(ctx.channel());
                handler.onNext(Pair.of(status, result));
                handler.onCompleted();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Attribute<SubscriptionCompletionHandler> handlerAttribute = ctx.attr(HANDLER_ATTRIBUTE_KEY);
        SubscriptionCompletionHandler handler = handlerAttribute.getAndRemove();

        ctx.close();
        handler.onError(cause);
    }
}
