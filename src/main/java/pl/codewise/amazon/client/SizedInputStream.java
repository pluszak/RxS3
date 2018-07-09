package pl.codewise.amazon.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.FilterInputStream;

public class SizedInputStream extends FilterInputStream {

    private final ByteBuf content;
    private boolean closed;

    public SizedInputStream(ByteBuf content) {
        super(new ByteBufInputStream(content));

        this.content = content;
    }

    public int getContentLength() {
        return content.readableBytes();
    }
}
