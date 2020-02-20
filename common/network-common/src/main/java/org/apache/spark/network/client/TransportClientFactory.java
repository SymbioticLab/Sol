/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.File;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import com.codahale.metrics.MetricSet;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.*;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 */
public class TransportClientFactory implements Closeable {

  /** A simple data structure to track the pool of clients between two peer nodes. */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final List<TransportClientBootstrap> clientBootstraps;
  // for medium size data transfer, also the default
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;  
  // for small size data transfer, e.g., broadcast
  private final ConcurrentHashMap<SocketAddress, ClientPool> dedicatedConnectionPool;
  // for large size data transfer, especially independent decoupling
  private final ConcurrentHashMap<SocketAddress, ClientPool> giantConnectionPool;

  /** Random number generator for picking connections between peers. */
  private final Random rand;
  private final int numConnectionsPerPeer;

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  private PooledByteBufAllocator pooledAllocator;
  private EventLoopGroup auxiliaryWorkerGroup;
  private PooledByteBufAllocator auxiliaryPooledAllocator;
  private final NettyMemoryMetrics metrics;
  private final AtomicInteger initialDedicatedPort;
  private final AtomicInteger initialPort;
  private final int auxiliaryPort;
  private final int auxiliaryNetEnabled;
  private final HashMap<String, String> auxiliaryNetMap;
  private final int fixedPort;

  public TransportClientFactory(
      TransportContext context,
      List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    this.conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    this.connectionPool = new ConcurrentHashMap<>();
    this.dedicatedConnectionPool = new ConcurrentHashMap<>();
    this.giantConnectionPool = new ConcurrentHashMap();
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();
    this.initialDedicatedPort = new AtomicInteger(this.conf.getInt("spark.dedicatedChannel.port", 36060));
    this.initialPort = new AtomicInteger(this.conf.getInt("spark.generalChannel.port", 55555));
    this.auxiliaryPort = this.conf.getInt("spark.auxiliaryPort.port", 57778);
    this.auxiliaryNetEnabled = this.conf.getInt("spark.auxiliaryNet.enabled", 0);
    this.fixedPort = this.conf.getInt("spark.fixedPort.enabled", 0);

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    this.workerGroup = NettyUtils.createEventLoop(
        ioMode,
        (int) Math.ceil(conf.clientThreads()/3),
        conf.getModuleName() + "-client");
    this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), false /* allowCache */, (int) Math.ceil(conf.clientThreads()/3));

    this.auxiliaryWorkerGroup = NettyUtils.createEventLoop(
        ioMode,
        (int) Math.ceil(conf.clientThreads() * 2/3),
        conf.getModuleName() + "-clientAuxiliary");

    this.auxiliaryPooledAllocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), false /* allowCache */, (int) Math.ceil(conf.clientThreads() *2/3));

    this.metrics = new NettyMemoryMetrics(
      this.pooledAllocator, conf.getModuleName() + "-client", conf);

    this.auxiliaryNetMap = new HashMap<>();

    if (this.auxiliaryNetEnabled == 1) {
      // load net ip maps
      try {
        File filename = new File("/mnt/tmp/auxiNet.cfg");
        InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
        BufferedReader br = new BufferedReader(reader);
        String line = br.readLine();
        while (line != null) {
          logger.info("====Try to convert {}", line);
          auxiliaryNetMap.put(line.trim().split("\\s+")[0], line.trim().split("\\s+")[1]);
          logger.info("====Convert auxiNet from {} to {}", line.trim().split("\\s+")[0], line.trim().split("\\s+")[1]);
          line = br.readLine();
        }
      } catch (IOException e) {

      }
    }
  }

  public MetricSet getAllMetrics() {
    return metrics;
  }


  public TransportClient createGaintClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    final InetSocketAddress unresolvedAddress =
      InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool = giantConnectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      giantConnectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
      clientPool = giantConnectionPool.get(unresolvedAddress);
      logger.info("Creating gaint clientPool in TransportClient for {}:{}", remoteHost, remotePort);
    }

    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler = cachedClient.getChannel().pipeline()
        .get(TransportChannelHandler.class);
      synchronized (handler) {
        handler.getResponseHandler().updateTimeOfLastRequest();
      }

      if (cachedClient.isActive()) {
        logger.info("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }

      logger.info("Creating clients in TransportClient for {}:{}", remoteHost, remotePort);
      clientPool.clients[clientIndex] = createClient(resolvedAddress, false);
      return clientPool.clients[clientIndex];
    }
  }


  public TransportClient createDedicatedClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    final InetSocketAddress unresolvedAddress =
      InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool = dedicatedConnectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      dedicatedConnectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
      clientPool = dedicatedConnectionPool.get(unresolvedAddress);
      logger.info("Creating dedicated clientPool in TransportClient for {}:{}", remoteHost, remotePort);
    }

    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler = cachedClient.getChannel().pipeline()
        .get(TransportChannelHandler.class);
      synchronized (handler) {
        handler.getResponseHandler().updateTimeOfLastRequest();
      }

      if (cachedClient.isActive()) {
        logger.info("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);

    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }

      logger.info("Creating clients in TransportClient for {}:{}", remoteHost, remotePort);
      clientPool.clients[clientIndex] = createClient(resolvedAddress, false);
      return clientPool.clients[clientIndex];
    }
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   *
   * Concurrency: This method is safe to call from multiple threads.
   */
  public TransportClient createClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    final InetSocketAddress unresolvedAddress =
      InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool = connectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
      clientPool = connectionPool.get(unresolvedAddress);
      logger.info("Creating clientPool in TransportClient for {}:{}", remoteHost, remotePort);
    }

    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler = cachedClient.getChannel().pipeline()
        .get(TransportChannelHandler.class);
      synchronized (handler) {
        handler.getResponseHandler().updateTimeOfLastRequest();
      }

      if (cachedClient.isActive()) {
        logger.info("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }

      logger.info("Creating clients in TransportClient for {}:{}", remoteHost, remotePort);
      clientPool.clients[clientIndex] = createClient(resolvedAddress, true);
      return clientPool.clients[clientIndex];
    }
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port.
   * This connection is not pooled.
   *
   * As with {@link #createClient(String, int)}, this method is blocking.
   */
  public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    return createClient(address, true);
  }


  /** Create a completely new {@link TransportClient} to the remote address. */
  private TransportClient createClient(InetSocketAddress address, boolean isGeneral)
      throws IOException, InterruptedException {
    logger.info("====Creating new connection directly via address to {}", address);

    int TCP_BUF = conf.getInt("spark.rpcBuffer.size", 10 * 1024 * 1024);

    Bootstrap bootstrap = new Bootstrap();

    if (TCP_BUF != 10 * 1024 * 1024) {
      logger.info("Now set TCP_BUF to {}", TCP_BUF);

      if (!isGeneral) {
        bootstrap.group(auxiliaryWorkerGroup)
          .channel(socketChannelClass)
          // Disable Nagle's Algorithm since we don't want packets to wait
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
          .option(ChannelOption.ALLOCATOR, auxiliaryPooledAllocator)
          .option(ChannelOption.SO_RCVBUF, TCP_BUF)
          .option(ChannelOption.SO_SNDBUF, TCP_BUF);
      } else {
        bootstrap.group(workerGroup)
          .channel(socketChannelClass)
          // Disable Nagle's Algorithm since we don't want packets to wait
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
          .option(ChannelOption.ALLOCATOR, pooledAllocator)
          .option(ChannelOption.SO_RCVBUF, TCP_BUF)
          .option(ChannelOption.SO_SNDBUF, TCP_BUF);
      }
    } else {
      if (!isGeneral) {
        bootstrap.group(auxiliaryWorkerGroup)
          .channel(socketChannelClass)
          // Disable Nagle's Algorithm since we don't want packets to wait
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
          .option(ChannelOption.ALLOCATOR, auxiliaryPooledAllocator);
      } else {
        bootstrap.group(workerGroup)
          .channel(socketChannelClass)
          // Disable Nagle's Algorithm since we don't want packets to wait
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
          .option(ChannelOption.ALLOCATOR, pooledAllocator);
      }
    }

    if (conf.receiveBuf() > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        clientRef.set(clientHandler.getClient());
        channelRef.set(ch);
      }
    });

    // Connect to the remote server
    long preConnect = System.nanoTime();

    // create dediated channel with given port
    InetSocketAddress localBindAddress = null;
    ChannelFuture cf;
    boolean finishPortSearch = false;

    do {
      try {
        if (fixedPort == 1) {
          if (!isGeneral){
            synchronized (initialDedicatedPort) {
              if (auxiliaryNetEnabled == 1) {
                logger.info("====Try to convert env {} to {}", System.getenv("SPARK_LOCAL_IP"), auxiliaryNetMap.get(System.getenv("SPARK_LOCAL_IP")));
                localBindAddress = new InetSocketAddress(auxiliaryNetMap.get(System.getenv("SPARK_LOCAL_IP")), initialDedicatedPort.incrementAndGet());
              }
              else
                localBindAddress = new InetSocketAddress(System.getenv("SPARK_LOCAL_IP"), initialDedicatedPort.incrementAndGet());
            }
          } else {
            synchronized (initialPort) {
              localBindAddress = new InetSocketAddress(System.getenv("SPARK_LOCAL_IP"), initialPort.incrementAndGet());
            }
          }

          if (!isGeneral) {
            if (auxiliaryNetEnabled == 1){
              logger.info("====Try to convert env {} to {}", address.getHostName(), auxiliaryNetMap.get(address.getHostName()));
              cf = bootstrap.connect(new InetSocketAddress(auxiliaryNetMap.get(address.getHostName()), auxiliaryPort), localBindAddress);
              logger.info("====Should have done connection");
            }
            else
              cf = bootstrap.connect(address, localBindAddress);
          } else {
            cf = bootstrap.connect(address, localBindAddress);
          }
        } else {
          cf = bootstrap.connect(address);
        }

        if (!cf.await(conf.connectionTimeoutMs())) {
          throw new IOException(
            String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
        } else if (cf.cause() != null) {
          logger.info("====Failed to connect to {} with {}, as {}", address, localBindAddress, cf.cause());
          throw new IOException(String.format("Failed to connect to %s with %s", address, localBindAddress), cf.cause());
        } else {
          finishPortSearch = true;
          logger.info("====isGeneral {}, Current bind address of cf is {}", isGeneral, cf.channel().localAddress());
        }
      } catch (Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        logger.info("====Error {}", sw.toString());
      }
    } while (!finishPortSearch);

    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();
    assert client != null : "Channel future completed successfully with null client";

    //logger.info("====Real channel address is {}", channel.localAddress());

    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        clientBootstrap.doBootstrap(client, channel);
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
      logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
      client.close();
      throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();

    logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
      address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

    return client;
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
      workerGroup = null;
    }

    if (auxiliaryWorkerGroup != null) {
      auxiliaryWorkerGroup.shutdownGracefully();
      auxiliaryWorkerGroup = null;
    }
  }
}
