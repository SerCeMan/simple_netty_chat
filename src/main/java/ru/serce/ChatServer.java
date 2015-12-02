package ru.serce;

import com.google.protobuf.CodedOutputStream;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author serce
 * @since 20.11.15.
 */
public class ChatServer {
    public static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    public static final EventLoopGroup bossGroup = new EpollEventLoopGroup(1);
    public static final EventLoopGroup workerGroup = new EpollEventLoopGroup();
    public static final long POLL_LIMIT = 250;
    public static final AtomicInteger sended = new AtomicInteger();
    public static final AtomicInteger tried = new AtomicInteger();
    public static final AtomicInteger hit = new AtomicInteger();
    public static final AtomicInteger poolg = new AtomicInteger();
    // todo persistent + CAS
    public static final ConcurrentHashMap<Channel, LinkedBlockingQueue<ByteBuf>> pendings = new ConcurrentHashMap<>();
    public static final int POOL_CAPACITY = 5000;
    // todo convinient pool
    public static final ArrayBlockingQueue<LinkedBlockingQueue<ByteBuf>> pool = new ArrayBlockingQueue<>(POOL_CAPACITY);

    static {
        new Thread(() -> {
            while (true) {
                try {
                    System.out.println("CHANS " + channels.size());
                    System.out.println("SENDED " + sended.get());
                    System.out.println("TRIED " + tried.get());
                    System.out.println("POOL RATIO " + hit.get() / (double) poolg.get());

                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    public static void main(String[] args) throws InterruptedException {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.TCP_NODELAY, true);
            b.group(bossGroup, workerGroup)
                    .channel(EpollServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                            cause.printStackTrace();
                            ctx.close();
                        }

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new ProtobufVarint32FrameDecoder());

                            p.addLast(new ByteBufLenghtBasedWriter());
                            p.addLast(new SimpleChannelInboundHandler<ByteBuf>(true) {

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    channels.add(ctx.channel());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                    Channel chan = ctx.channel();
                                    channels.remove(chan);
                                    LinkedBlockingQueue<ByteBuf> old = pendings.remove(chan);
                                    if (old != null) {
                                        old.clear();
                                        pool.offer(old);
                                    }
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                                    tried.incrementAndGet();
                                    Channel ctxChan = ctx.channel();
                                    for (Channel ch : channels) {
                                        if (ch != ctxChan) {
                                            msg.retain();
                                            putInChan(msg, ch, true);
                                        }
                                    }
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });

            workerGroup.schedule(ChatServer::handlePendings, 0, TimeUnit.MILLISECONDS);

            b.bind(20026).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static void putInChan(ByteBuf msg, Channel ch, boolean flush) {
        if (ch.isWritable()) {
            if (flush) {
                ch.writeAndFlush(msg, ch.voidPromise());
            } else {
                ch.write(msg, ch.voidPromise());
            }
        } else if (ch.isOpen()) {
            LinkedBlockingQueue<ByteBuf> queue = pendings.computeIfAbsent(ch, channel -> getQueue());
            try {
                queue.put(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static LinkedBlockingQueue<ByteBuf> getQueue() {
        poolg.incrementAndGet();
        LinkedBlockingQueue<ByteBuf> q = pool.poll();
        if (q != null) {
            hit.incrementAndGet();
            return q;
        }
        return new LinkedBlockingQueue<>();
    }

    private static void handlePendings() {
        boolean hasWork = false;
        for (Map.Entry<Channel, LinkedBlockingQueue<ByteBuf>> entry : pendings.entrySet()) {
            Channel chan = entry.getKey();
            LinkedBlockingQueue<ByteBuf> queue = entry.getValue();
            ArrayList<ByteBuf> messages = pollFromQueue(queue, POLL_LIMIT);
            if (messages != null) {
                hasWork = true;
                chan.eventLoop().execute(() -> {
                    for (int i = 0; i < messages.size(); i++) {
                        ByteBuf msg = messages.get(i);
                        int last = messages.size() - 1;
                        boolean flush = i == last;
                        putInChan(msg, chan, flush);
                    }
                });
            }
        }
        if (hasWork) {
            workerGroup.schedule(ChatServer::handlePendings, 0, TimeUnit.MILLISECONDS);
        } else {
            workerGroup.schedule(ChatServer::handlePendings, 50, TimeUnit.MILLISECONDS);
        }
    }

    private static ArrayList<ByteBuf> pollFromQueue(LinkedBlockingQueue<ByteBuf> queue, long limit) {
        ArrayList<ByteBuf> res = null;
        while (true) {
            ByteBuf val = queue.poll();
            if (val == null) {
                break;
            }
            if (res == null) {
                res = new ArrayList<>(queue.size() + 1);
            }
            res.add(val);
            if (res.size() >= limit) {
                break;
            }
        }
        return res;
    }

    public static void writeRawVarint32(ByteBuf buf, int value) throws IOException {
        while (true) {
            if ((value & ~0x7F) == 0) {
                buf.writeByte((byte) value);
                return;
            } else {
                buf.writeByte((byte) (value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    @ChannelHandler.Sharable
    private static class ByteBufLenghtBasedWriter extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            ByteBuf buf = null;
            try {
                ByteBuf cast = (ByteBuf) msg;
                buf = PooledByteBufAllocator.DEFAULT.ioBuffer();

                int bodyLen = cast.readableBytes();
                int headerLen = CodedOutputStream.computeRawVarint32Size(bodyLen);
                buf.ensureWritable(headerLen + bodyLen);
                writeRawVarint32(buf, bodyLen);
                try {
                    buf.writeBytes(cast, buf.readerIndex(), bodyLen);
                } finally {
                    cast.release();
                }

                if (buf.isReadable()) {
                    ctx.write(buf, promise);
                } else {
                    buf.release();
                    ctx.write(Unpooled.EMPTY_BUFFER, promise);
                }
                buf = null;
            } catch (EncoderException e) {
                e.printStackTrace();
                throw e;
            } catch (Throwable e) {
                e.printStackTrace();
                throw new EncoderException(e);
            } finally {
                if (buf != null) {
                    buf.release();
                }
            }
        }
    }
}
