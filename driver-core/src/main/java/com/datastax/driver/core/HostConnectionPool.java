/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.OneTimeTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.MoreFutures;

class HostConnectionPool implements Connection.Owner {
    private static final Logger logger = LoggerFactory.getLogger(HostConnectionPool.class);

    final Host host;
    volatile HostDistance hostDistance;
    private final SessionManager manager;

    // To remove the need for complex synchronization, all administrative operations are confined to a single thread.
    private final EventExecutor adminThread;

    @VisibleForTesting
    final List<Connection> connections;
    @VisibleForTesting
    final Set<Connection> trash;

    // Maintain these separately for metrics (volatile allows querying them outside of adminThread)
    private volatile int connectionCount;
    private volatile int trashedCount;
    private volatile int totalInFlight;

    private final Queue<BorrowTask> pendingBorrowQueue = new ArrayDeque<BorrowTask>();

    protected final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    private enum Phase {INITIALIZING, READY, INIT_FAILED, CLOSING}

    private Phase phase = Phase.INITIALIZING;

    // When a request times out, we may never release its stream ID. So over time, a given connection
    // may get less an less available streams. When the number of available ones go below the
    // following threshold, we just replace the connection by a new one.
    private final int minAllowedStreams;

    /** The maximum total inFlight since the last call execution of {@link CleanupTask} */
    private int maxTotalInFlight = 0;

    private ListenableFuture<Void> newConnection;
    private final ScheduledFuture<?> cleanup;

    HostConnectionPool(Host host, HostDistance hostDistance, SessionManager manager) {
        this.host = host;
        this.hostDistance = hostDistance;
        this.manager = manager;
        this.connections = Lists.newArrayList();
        this.trash = Sets.newHashSet();

        this.adminThread = manager.connectionFactory().eventLoopGroup.next();

        this.minAllowedStreams = options().getMaxRequestsPerConnection(hostDistance) * 3 / 4;

        this.cleanup = this.adminThread.scheduleWithFixedDelay(new CleanupTask(), 10, 10, TimeUnit.SECONDS);
    }

    /**
     * @param reusedConnection an existing connection (from a reconnection attempt) that we want to
     *                         reuse as part of this pool. Might be null or already used by another
     *                         pool.
     */
    ListenableFuture<Void> initAsync(final Connection reusedConnection) {
        final SettableFuture<Void> initFuture = SettableFuture.create();
        if (adminThread.inEventLoop()) {
            safeInitAsync(reusedConnection, initFuture);
        } else {
            adminThread.execute(new OneTimeTask() {
                @Override
                public void run() {
                    safeInitAsync(reusedConnection, initFuture);
                }
            });
        }
        return initFuture;
    }

    private void safeInitAsync(Connection reusedConnection, final SettableFuture<Void> initFuture) {
        assert adminThread.inEventLoop();

        // Create initial core connections
        final int coreSize = options().getCoreConnectionsPerHost(hostDistance);
        final List<Connection> connections = Lists.newArrayListWithCapacity(coreSize);
        final List<ListenableFuture<Void>> connectionFutures = Lists.newArrayListWithCapacity(coreSize);
        for (int i = 0; i < coreSize; i++) {
            Connection connection;
            ListenableFuture<Void> connectionFuture;
            // reuse the existing connection only once
            if (reusedConnection != null && reusedConnection.setOwner(this)) {
                connection = reusedConnection;
                connectionFuture = MoreFutures.VOID_SUCCESS;
            } else {
                connection = manager.connectionFactory().newConnection(this);
                connectionFuture = connection.initAsync();
            }
            reusedConnection = null;
            connections.add(connection);
            connectionFutures.add(handleErrors(connectionFuture));
        }

        ListenableFuture<List<Void>> allConnectionsFuture = Futures.allAsList(connectionFutures);

        Futures.addCallback(allConnectionsFuture, new FutureCallback<List<Void>>() {
            @Override
            public void onSuccess(List<Void> l) {
                // Some of the connections might have failed, keep only the successful ones
                ListIterator<Connection> it = connections.listIterator();
                while (it.hasNext()) {
                    if (it.next().isClosed())
                        it.remove();
                }

                HostConnectionPool.this.connections.addAll(connections);
                connectionCount = connections.size();

                if (isClosed()) {
                    initFuture.setException(new ConnectionException(host.getSocketAddress(), "Pool was closed during initialization"));
                    // we're not sure if closeAsync() saw the connections, so ensure they get closed
                    forceClose(connections);
                } else {
                    logger.debug("Created connection pool to host {} ({} connections needed, {} successfully opened)",
                        host, coreSize, connectionCount);
                    phase = Phase.READY;
                    initFuture.set(null);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                phase = Phase.INIT_FAILED;
                forceClose(connections);
                initFuture.setException(t);
            }
        }, adminThread);
    }

    private ListenableFuture<Void> handleErrors(ListenableFuture<Void> connectionInitFuture) {
        return Futures.withFallback(connectionInitFuture, new FutureFallback<Void>() {
            @Override
            public ListenableFuture<Void> create(Throwable t) throws Exception {
                // Propagate these exceptions because they mean no connection will ever succeed. They will be handled
                // accordingly in SessionManager#maybeAddPool.
                Throwables.propagateIfInstanceOf(t, ClusterNameMismatchException.class);
                Throwables.propagateIfInstanceOf(t, UnsupportedProtocolVersionException.class);
                Throwables.propagateIfInstanceOf(t, SetKeyspaceException.class);

                // We don't want to swallow Errors either as they probably indicate a more serious issue (OOME...)
                Throwables.propagateIfInstanceOf(t, Error.class);

                // Otherwise, return success. The pool will simply ignore this connection when it sees that it's been closed.
                return MoreFutures.VOID_SUCCESS;
            }
        });
    }

    // Clean up if we got a fatal error at construction time but still created part of the core connections
    private void forceClose(List<Connection> connections) {
        for (Connection connection : connections) {
            connection.closeAsync().force();
        }
    }

    /**
     * @deprecated this method is provided temporarily to integrate with existing synchronous code. Eventually all
     * clients should be refactored to use {@link #borrowConnectionAsync(long, TimeUnit)} (which should then be renamed).
     */
    @Deprecated
    Connection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(borrowConnectionAsync(timeout, unit));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.propagateIfInstanceOf(cause, ConnectionException.class);
            Throwables.propagateIfInstanceOf(cause, TimeoutException.class);
            throw Throwables.propagate(cause);
        }
    }

    ListenableFuture<Connection> borrowConnectionAsync(final long timeout, final TimeUnit unit) {
        final SettableFuture<Connection> connectionFuture = SettableFuture.create();
        if (adminThread.inEventLoop()) {
            safeBorrow(timeout, unit, connectionFuture);
        } else {
            adminThread.execute(new OneTimeTask() {
                @Override
                public void run() {
                    safeBorrow(timeout, unit, connectionFuture);
                }
            });
        }
        return connectionFuture;
    }

    private void safeBorrow(long timeout, TimeUnit unit, final SettableFuture<Connection> connectionFuture) {
        assert adminThread.inEventLoop();

        if (phase != Phase.READY) {
            connectionFuture.setException(new ConnectionException(host.getSocketAddress(), "Pool is " + phase));
        }

        if (connections.isEmpty()) {
            // If core pool size = 0, trigger a new connection. In other cases, the pool should already be doing it.
            if (options().getCoreConnectionsPerHost(hostDistance) == 0 && host.convictionPolicy.canReconnectNow()) {
                maybeSpawnNewConnection();
            }
            enqueue(connectionFuture, timeout, unit);
            return;
        }

        int minInFlight = Integer.MAX_VALUE;
        Connection leastBusy = null;
        for (Connection connection : connections) {
            int inFlight = connection.inFlight;
            if (inFlight < minInFlight) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
        }

        if (leastBusy == null || minInFlight >= Math.min(leastBusy.maxAvailableStreams(), options().getMaxRequestsPerConnection(hostDistance))) {
            // Note: the queue is unbounded (like in the previous implementation of the pool),
            // but a limit to the pending number of borrows could easily be added
            enqueue(connectionFuture, timeout, unit);
        } else {
            // prepare connection before returning to client
            onBorrowed(connectionFuture, leastBusy);
        }
    }

    private void enqueue(SettableFuture<Connection> connectionFuture, long timeout, TimeUnit unit) {
        assert adminThread.inEventLoop();

        if (timeout == 0) {
            connectionFuture.setException(new TimeoutException("No connection immediately available and pool timeout is 0"));
        } else {
            pendingBorrowQueue.offer(
                new BorrowTask(connectionFuture, TimeUnit.NANOSECONDS.convert(timeout, unit)));
        }
    }

    private void onBorrowed(final SettableFuture<Connection> connectionFuture, final Connection connection) {
        assert adminThread.inEventLoop();

        connection.inFlight += 1;
        totalInFlight += 1;

        maxTotalInFlight = Math.max(maxTotalInFlight, totalInFlight);

        int connectionCount = this.connectionCount +
            (newConnection != null && !newConnection.isDone() ? 1 : 0);
        if (connectionCount < options().getCoreConnectionsPerHost(hostDistance)) {
            maybeSpawnNewConnection();
        } else if (connectionCount < options().getMaxConnectionsPerHost(hostDistance)) {
            // Add a connection if we fill the first n-1 connections and almost fill the last one
            int currentCapacity = (connectionCount - 1) * options().getMaxRequestsPerConnection(hostDistance)
                + options().getNewConnectionThreshold(hostDistance);
            if (totalInFlight > currentCapacity)
                maybeSpawnNewConnection();
        }

        try {
            Futures.addCallback(connection.setKeyspaceAsync(manager.poolsState.keyspace), new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    connectionFuture.set(connection);
                }

                @Override
                public void onFailure(Throwable t) {
                    connectionFuture.setException(t);
                }
            }, adminThread);
        } catch (ConnectionException e) {
            connectionFuture.setException(e);
        } catch (BusyConnectionException e) {
            connectionFuture.setException(e);
        }
    }

    private void maybeSpawnNewConnection() {
        assert adminThread.inEventLoop();

        if (!host.convictionPolicy.canReconnectNow())
            return;

        // Abort if we're already in the process of adding a connection
        if (newConnection != null && !newConnection.isDone())
            return;

        if (connectionCount >= options().getMaxConnectionsPerHost(hostDistance))
            return;

        newConnection = spawnNewConnection();
    }

    private ListenableFuture<Void> spawnNewConnection() {
        assert adminThread.inEventLoop();

        if (connectionCount >= options().getMaxConnectionsPerHost(hostDistance))
            return MoreFutures.VOID_SUCCESS;

        Connection resurrected = tryResurrectFromTrash();
        if (resurrected != null) {
            // Successfully resurrected from trash, it's already initialized so we can dequeue immediately
            connections.add(resurrected);
            connectionCount = connections.size();
            dequeuePendingBorrows(resurrected);
            return MoreFutures.VOID_SUCCESS;
        } else if (!host.convictionPolicy.canReconnectNow()) {
            return MoreFutures.VOID_SUCCESS;
        } else {
            logger.debug("Creating new connection on busy pool to {}", host);
            final Connection connection = manager.connectionFactory().newConnection(this);
            ListenableFuture<Void> initFuture = connection.initAsync();
            initFuture.addListener(new Runnable() {
                @Override
                public void run() {
                    connections.add(connection);
                    connectionCount = connections.size();
                    dequeuePendingBorrows(connection);
                }
            }, adminThread);
            return initFuture;
        }
    }

    private Connection tryResurrectFromTrash() {
        assert adminThread.inEventLoop();

        long highestMaxIdleTime = System.currentTimeMillis();
        Connection chosen = null;

        for (Connection connection : trash)
            if (connection.maxIdleTime > highestMaxIdleTime && connection.maxAvailableStreams() > minAllowedStreams) {
                chosen = connection;
                highestMaxIdleTime = connection.maxIdleTime;
            }

        if (chosen == null)
            return null;

        logger.trace("Resurrecting {}", chosen);
        trash.remove(chosen);
        trashedCount = trash.size();
        return chosen;
    }

    private void dequeuePendingBorrows(Connection connection) {
        assert adminThread.inEventLoop();

        while (connection.inFlight < connection.maxAvailableStreams()) {
            BorrowTask borrowTask = pendingBorrowQueue.poll();
            if (borrowTask == null)
                break;

            borrowTask.timeoutFuture.cancel(false);
            onBorrowed(borrowTask.future, connection);
        }
    }

    void returnConnection(final Connection connection) {
        if (adminThread.inEventLoop()) {
            safeReturn(connection);
        } else {
            adminThread.execute(new OneTimeTask() {
                @Override
                public void run() {
                    safeReturn(connection);
                }
            });
        }
    }

    private void safeReturn(Connection connection) {
        assert adminThread.inEventLoop();

        connection.inFlight -= 1;
        totalInFlight -= 1;

        if (connection.isDefunct()) {
            // As part of making it defunct, we have already replaced it or closed the pool.
            return;
        }

        if (connection.maxAvailableStreams() < minAllowedStreams) {
            replaceConnection(connection);
        } else {
            dequeuePendingBorrows(connection);
        }
    }

    private void replaceConnection(Connection connection) {
        assert adminThread.inEventLoop();

        maybeSpawnNewConnection();
        connection.maxIdleTime = Long.MIN_VALUE;
        doTrashConnection(connection);
    }

    private void doTrashConnection(Connection connection) {
        assert adminThread.inEventLoop();

        connections.remove(connection);
        trash.add(connection);
        connectionCount = connections.size();
        trashedCount = connections.size();
    }

    @Override
    public void onConnectionDefunct(final Connection connection) {
        if (adminThread.inEventLoop()) {
            // Don't try to replace the connection now. Connection.defunct already signaled the failure,
            // and either the host will be marked DOWN (which destroys all pools), or we want to prevent
            // new connections for some time
            connections.remove(connection);
            connectionCount = connections.size();
        } else {
            adminThread.execute(new OneTimeTask() {
                @Override
                public void run() {
                    connections.remove(connection);
                    connectionCount = connections.size();
                }
            });
        }
    }

    private PoolingOptions options() {
        return manager.configuration().getPoolingOptions();
    }

    private class BorrowTask {
        private final SettableFuture<Connection> future;
        private final long expireNanoTime;
        private final ScheduledFuture<?> timeoutFuture;

        public BorrowTask(SettableFuture<Connection> future, long timeout) {
            this.future = future;
            this.expireNanoTime = System.nanoTime() + timeout;
            this.timeoutFuture = adminThread.schedule(timeoutTask, timeout, TimeUnit.NANOSECONDS);
        }
    }

    private Runnable timeoutTask = new Runnable() {
        @Override
        public void run() {
            assert adminThread.inEventLoop();
            long nanoTime = System.nanoTime();
            while (true) {
                BorrowTask task = pendingBorrowQueue.peek();
                if (task == null || nanoTime - task.expireNanoTime < 0) {
                    break;
                }
                pendingBorrowQueue.remove();
                task.future.setException(new TimeoutException("Could not acquire connection within the given timeout"));
            }
        }
    };

    private class CleanupTask implements Runnable {
        @Override
        public void run() {
            assert adminThread.inEventLoop();

            if (isClosed())
                return;

            shrinkIfBelowCapacity();
            cleanupTrash(System.currentTimeMillis());
        }
    }

    /** If we have more active connections than needed, trash some of them */
    private void shrinkIfBelowCapacity() {
        int currentLoad = maxTotalInFlight;
        maxTotalInFlight = totalInFlight;

        int maxRequestsPerConnection = options().getMaxRequestsPerConnection(hostDistance);
        int needed = currentLoad / maxRequestsPerConnection + 1;
        if (currentLoad % maxRequestsPerConnection > options().getNewConnectionThreshold(hostDistance))
            needed += 1;
        needed = Math.max(needed, options().getCoreConnectionsPerHost(hostDistance));
        int actual = connectionCount;
        int toTrash = Math.max(0, actual - needed);

        logger.trace("Current inFlight = {}, {} connections needed, {} connections available, trashing {}",
            currentLoad, needed, actual, toTrash);

        if (toTrash <= 0)
            return;

        for (Connection connection : connections)
            if (trashConnection(connection)) {
                toTrash -= 1;
                if (toTrash == 0)
                    return;
            }
    }

    /** Close connections that have been sitting in the trash for too long */
    private void cleanupTrash(long now) {
        for (Connection connection : trash) {
            if (connection.maxIdleTime < now) {
                logger.trace("Cleaning up {}", connection);
                trash.remove(connection);
                connection.closeAsync();
            }
        }
        trashedCount = trash.size();
    }

    private boolean trashConnection(Connection connection) {
        assert adminThread.inEventLoop();

        if (connectionCount <= options().getCoreConnectionsPerHost(hostDistance))
            return false;

        connection.maxIdleTime = System.currentTimeMillis() + options().getIdleTimeoutSeconds() * 1000;
        doTrashConnection(connection);
        return true;
    }

    CloseFuture closeAsync() {
        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        final CloseFuture.Forwarding myFuture = new CloseFuture.Forwarding();
        if (closeFuture.compareAndSet(null, myFuture)) {
            if (adminThread.inEventLoop()) {
                safeClose(myFuture);
            } else {
                adminThread.execute(new OneTimeTask() {
                    @Override
                    public void run() {
                        safeClose(myFuture);
                    }
                });
            }
            return myFuture;
        } else {
            return closeFuture.get();
        }
    }

    private void safeClose(CloseFuture.Forwarding forwardingFuture) {
        assert adminThread.inEventLoop();

        phase = Phase.CLOSING;

        cleanup.cancel(false);

        // Notify all clients that were waiting for a connection
        BorrowTask borrowTask;
        while ((borrowTask = pendingBorrowQueue.poll()) != null) {
            borrowTask.timeoutFuture.cancel(false);
            borrowTask.future.setException(new ConnectionException(host.getSocketAddress(), "Pool is " + phase));
        }

        forwardingFuture.setFutures(discardAvailableConnections());
    }

    private List<CloseFuture> discardAvailableConnections() {
        assert adminThread.inEventLoop();

        // Note: if this gets called before initialization has completed, both connections and trash will be empty,
        // so this will return an empty list
        List<CloseFuture> futures = new ArrayList<CloseFuture>(connectionCount + trashedCount);

        for (final Connection connection : connections) {
            CloseFuture future = connection.closeAsync();
            futures.add(future);
        }

        // Some connections in the trash might still be open if they hadn't reached their idle timeout
        for (Connection connection : trash)
            futures.add(connection.closeAsync());

        return futures;
    }

    final boolean isClosed() {
        return closeFuture.get() != null;
    }

    /**
     * Creates connections if we have less than core connections (if we
     * have more than core, connection will just get trashed when we can).
     */
    void ensureCoreConnections() {
        if (isClosed())
            return;

        if (!host.convictionPolicy.canReconnectNow())
            return;

        if (adminThread.inEventLoop()) {
            safeEnsureCoreConnections();
        } else {
            adminThread.execute(new OneTimeTask() {
                @Override
                public void run() {
                    safeEnsureCoreConnections();
                }
            });
        }
    }

    private void safeEnsureCoreConnections() {
        assert adminThread.inEventLoop();

        int needed = options().getCoreConnectionsPerHost(hostDistance) - connectionCount;
        for (int i = 0; i < needed; i++) {
            spawnNewConnection();
        }
    }

    int opened() {
        return connectionCount;
    }

    int trashed() {
        return trashedCount;
    }

    int totalInFlight() {
        return totalInFlight;
    }

    static class PoolState {
        volatile String keyspace;

        PoolState(String keyspace) {
            this.keyspace = keyspace;
        }

        void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }
    }
}
