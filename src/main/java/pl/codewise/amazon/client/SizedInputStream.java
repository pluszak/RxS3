package pl.codewise.amazon.client;

import java.io.FilterInputStream;
import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.ReferenceCountUtil;

public class SizedInputStream extends FilterInputStream {

    private final ByteBuf content;

    public SizedInputStream(ByteBuf content) {
        super(new ByteBufInputStream(content));

        this.content = content;
    }

    public int getContentLength() {
        return content.readableBytes();
    }

    @Override
    public void close() throws IOException {
        super.close();
        ReferenceCountUtil.release(content);
    }
}
