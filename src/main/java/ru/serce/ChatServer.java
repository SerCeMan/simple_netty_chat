package ru.serce;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.concurrent.EventExecutor;
import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author serce
 * @since 20.11.15.
 */
public class ChatServer {
    public static final ThreadLocal<HashSet<Channel>> channels = new ThreadLocal<>().withInitial(() -> new HashSet<>(500));
    public static final EventLoopGroup bossGroup = new EpollEventLoopGroup(1);
    public static EpollEventLoopGroup workerGroup = null;
    public static final AtomicInteger tried = new AtomicInteger();
    public static final LinkedBlockingQueue<Pair<Channel, ByteBuf>> commands = new LinkedBlockingQueue<>();

    static {
        new Thread(() -> {
            while (true) {
                try {
                    System.out.println("TRIED " + tried.get());

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
            int nThreads = 1;
            if (args.length > 0) {
                nThreads = Integer.parseInt(args[0]);
            } else {
                nThreads = Runtime.getRuntime().availableProcessors();
            }
            workerGroup = new EpollEventLoopGroup(nThreads);
            ByteBufSimpleChannelInboundHandler inboundHandler = new ByteBufSimpleChannelInboundHandler();
            ServerBootstrap b = new ServerBootstrap();
            b.option(EpollChannelOption.TCP_CORK, true);
            b.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
            b.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 16 * 1024);
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
                            MyProtobufVarint32FrameDecoder decoder = new MyProtobufVarint32FrameDecoder();
                            p.addLast(decoder);
                            p.addLast(inboundHandler);
                        }
                    });


            for (EventExecutor executor : workerGroup) {
                executor.schedule(() -> handlePendings(executor), 0, TimeUnit.MILLISECONDS);
            }
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

    private static void handlePendings(EventExecutor executor) {
        try {
            for (Channel chan : channels.get()) {
                if(chan.unsafe().outboundBuffer().totalPendingWriteBytes() > 0) {
                    chan.unsafe().flush();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        executor.schedule(() -> handlePendings(executor), 100, TimeUnit.MILLISECONDS);
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

    public static class MyProtobufVarint32FrameDecoder extends ByteToMessageDecoder {


        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            in.markReaderIndex();
            final byte[] buf = new byte[5];
            for (int i = 0; i < buf.length; i++) {
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
            channels.get().add(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            Channel chan = ctx.channel();
            channels.get().remove(chan);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            tried.incrementAndGet();
            Channel ctxChan = ctx.channel();
            if (isMessage(msg)) {
                msg.resetReaderIndex();
                for (EventExecutor eventExecutor : workerGroup) {
                    if (eventExecutor.inEventLoop()) {
                        handle(msg, ctxChan, eventExecutor);
                    } else {
                        msg.retain();
                        eventExecutor.submit(() -> {
                            handle(msg, ctxChan, eventExecutor);
                            msg.release();
                        });
                    }
                }
            } else {
                msg.retain();
                commands.put(new Pair<>(ctxChan, msg));
            }
        }

        private void handle(ByteBuf msg, Channel ctxChan, EventExecutor executor) {
            for (Channel ch : channels.get()) {
                if (ch != ctxChan) {
                    msg.retain();
                    ch.unsafe().write(msg, ch.voidPromise());
                    if (!ch.isWritable()) {
                        ch.unsafe().flush();
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }

}
