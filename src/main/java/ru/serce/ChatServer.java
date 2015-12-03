package ru.serce;

import com.google.protobuf.CodedInputStream;
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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.EncoderException;
import io.netty.util.concurrent.GlobalEventExecutor;
import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author serce
 * @since 20.11.15.
 */
public class ChatServer {
    public static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    public static final EventLoopGroup bossGroup = new EpollEventLoopGroup(1);
    public static final EventLoopGroup workerGroup = new EpollEventLoopGroup();
    public static final AtomicInteger tried = new AtomicInteger();
    public static final AtomicInteger can = new AtomicInteger();
    public static final AtomicInteger not = new AtomicInteger();
    public static final ConcurrentHashMap<Channel, AtomicReference<ConsPStack<ByteBuf>>> pendings = new ConcurrentHashMap<>();
    public static final LinkedBlockingQueue<Pair<Channel, ByteBuf>> commands = new LinkedBlockingQueue<>();

    static {
        new Thread(() -> {
            while (true) {
                try {
                    System.out.println("CHANS " + channels.size());
                    System.out.println("TRIED " + tried.get());
                    System.out.println("CAN " + can.get());
                    System.out.println("NOT " + not.get());

                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


        new Thread(() -> {
            final ChatProtocol.Message prototype = ChatProtocol.Message.getDefaultInstance().getDefaultInstanceForType();
            while (true) {
                Pair<Channel, ByteBuf> pair = null;
                ByteBuf msg = null;
                try {
                    pair = commands.take();

                    Channel chan = pair.getKey();
                    msg = pair.getValue();


                    final byte[] array;
                    final int offset;
                    final int length = msg.readableBytes();
                    if (msg.hasArray()) {
                        array = msg.array();
                        offset = msg.arrayOffset() + msg.readerIndex();
                    } else {
                        array = new byte[length];
                        msg.getBytes(msg.readerIndex(), array, 0, length);
                        offset = 0;
                    }

                    try {
                        ChatProtocol.Message res = prototype.getParserForType().parseFrom(array, offset, length);
                        Process exec = Runtime.getRuntime().exec(res.getTextList().get(0));
                        List<String> list = read(exec.getInputStream());
                        byte[] body = ChatProtocol.Message.newBuilder()
                                .setType(ChatProtocol.Message.Type.MESSAGE)
                                .setAuthor("Server")
                                .addAllText(list)
                                .build()
                                .toByteArray();
                        int bodyLen = body.length;
                        int headerLen = CodedOutputStream.computeRawVarint32Size(body.length);
                        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
                        buf.ensureWritable(headerLen + bodyLen);
                        writeRawVarint32(buf, bodyLen);
                        buf.writeBytes(body);
                        chan.writeAndFlush(buf);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (msg != null) {
                        msg.release();
                    }
                }


            }
        }).start();
    }

    public static List<String> read(InputStream input) throws IOException {
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
            return buffer.lines().collect(Collectors.toList());
        }
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
                            p.addLast(new MyProtobufVarint32FrameDecoder());

                            p.addLast(new ByteBufLenghtBasedWriter());
                            p.addLast(new ByteBufSimpleChannelInboundHandler());
                        }
                    });

            workerGroup.schedule(ChatServer::handlePendings, 0, TimeUnit.MILLISECONDS);

            b.bind(20053).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static boolean isMessage(ByteBuf msg) {
        int i = msg.readerIndex();
        return msg.array()[i + 1] != 1;
    }

    private static boolean putInChan(ByteBuf msg, Channel ch, boolean flush, AtomicReference<ConsPStack<ByteBuf>> ref) {
        if (ch.isWritable()) {
            if (flush) {
                ch.writeAndFlush(msg, ch.voidPromise());
            } else {
                ch.write(msg, ch.voidPromise());
            }
            return true;
        } else if (ch.isOpen()) {
            if (ref == null) {
                ref = pendings.computeIfAbsent(ch, ChatServer::getQueue);
            }
            put(msg, ref);
            return false;
        }
        return true;
    }

    private static void put(ByteBuf msg, AtomicReference<ConsPStack<ByteBuf>> ref) {
        ConsPStack<ByteBuf> oldStack;
        ConsPStack<ByteBuf> newStack;
        do {
            oldStack = ref.get();
            ConsPStack<ByteBuf> stack = oldStack;
            if (stack == null) {
                stack = ConsPStack.empty();
            }
            newStack = stack.plus(msg);
        } while (!ref.compareAndSet(oldStack, newStack));
    }

    private static ConsPStack<ByteBuf> extract(AtomicReference<ConsPStack<ByteBuf>> ref) {
        ConsPStack<ByteBuf> stack;
        do {
            stack = ref.get();
        } while (!ref.compareAndSet(stack, ConsPStack.empty()));
        return stack;
    }


    private static AtomicReference<ConsPStack<ByteBuf>> getQueue(Channel ch) {
        return new AtomicReference<>(ConsPStack.empty());
    }

    private static void handlePendings() {
        boolean hasWork = false;
        for (Map.Entry<Channel, AtomicReference<ConsPStack<ByteBuf>>> entry : pendings.entrySet()) {
            Channel chan = entry.getKey();
            AtomicReference<ConsPStack<ByteBuf>> ref = entry.getValue();
            ConsPStack<ByteBuf> messages = extract(ref);
            if (messages != null && !messages.isEmpty()) {
                hasWork = true;
                chan.eventLoop().execute(() -> {
                    ConsPStack<ByteBuf> empty = ConsPStack.<ByteBuf>empty();
                    ConsPStack<ByteBuf> current = messages;
                    boolean hasNext;
                    do {
                        ConsPStack<ByteBuf> orig = current;
                        ByteBuf msg = current.first;

                        current = current.rest;
                        hasNext = current != null && current != empty;

                        boolean flush = !hasNext;
                        if (chan.isWritable()) {
                            if (flush) {
                                chan.writeAndFlush(msg, chan.voidPromise());
                            } else {
                                chan.write(msg, chan.voidPromise());
                            }
                        } else if (chan.isOpen()) {
                            current = orig;
                            if (!ref.compareAndSet(empty, current)) {
                                do {
                                    msg = current.first;
                                    put(msg, ref);
                                    current = current.rest;
                                } while (current != null && current != empty);
                            }
                            hasNext = false;
                            chan.flush();
                        }
                    } while (hasNext);
                });
            }
        }
        if (hasWork) {
            workerGroup.schedule(ChatServer::handlePendings, 0, TimeUnit.MILLISECONDS);
        } else {
            workerGroup.schedule(ChatServer::handlePendings, 50, TimeUnit.MILLISECONDS);
        }
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
                buf = cast.slice();// PooledByteBufAllocator.DEFAULT.ioBuffer();

//                int bodyLen = cast.readableBytes();
//                int headerLen = CodedOutputStream.computeRawVarint32Size(bodyLen);
//                buf.ensureWritable(headerLen + bodyLen);
//                writeRawVarint32(buf, bodyLen);
//                try {
//                    buf.writeBytes(cast, buf.readerIndex(), bodyLen);
//                } finally {
//                    cast.release();
//                }

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

    public static class MyProtobufVarint32FrameDecoder extends ByteToMessageDecoder {


        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            in.markReaderIndex();
            final byte[] buf = new byte[5];
            for (int i = 0; i < buf.length; i ++) {
                if (!in.isReadable()) {
                    in.resetReaderIndex();
                    return;
                }

                buf[i] = in.readByte();
                if (buf[i] >= 0) {
                    int length = CodedInputStream.newInstance(buf, 0, i + 1).readRawVarint32();
                    if (length < 0) {
                        throw new CorruptedFrameException("negative length: " + length);
                    }

                    if (in.readableBytes() < length) {
                        in.resetReaderIndex();
                        return;
                    } else {
                        int len = i + 1;
                        in.resetReaderIndex();
                        ByteBuf e = in.readBytes(len + length);
                        e.readerIndex(len);
                        out.add(e);
                        return;
                    }
                }
            }

            // Couldn't find the byte whose MSB is off.
            throw new CorruptedFrameException("length wider than 32-bit");
        }
    }

    @ChannelHandler.Sharable
    private static class ByteBufSimpleChannelInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {

        public ByteBufSimpleChannelInboundHandler() {
            super(true);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channels.add(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            Channel chan = ctx.channel();
            channels.remove(chan);
            pendings.remove(chan);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            tried.incrementAndGet();
            Channel ctxChan = ctx.channel();
            if (isMessage(msg)) {
                msg.resetReaderIndex();
                for (Channel ch : channels) {
                    if (ch != ctxChan) {
                        msg.retain();
                        put(msg, pendings.computeIfAbsent(ch, ChatServer::getQueue));
//                        putInChan(msg, ch, true, null);
                    }
                }
            } else {
                msg.retain();
                commands.put(new Pair<>(ctxChan, msg));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
