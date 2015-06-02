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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.exceptions.DiscoveryException;
import com.datastax.driver.core.policies.DiscoveryPolicy;

/**
 * Default discovery strategy.
 * Relies on system.peers table of the control connection host to discover peers in the cluster.
 */
public class DefaultDiscoveryPolicy implements DiscoveryPolicy {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDiscoveryPolicy.class);

    private static final String SELECT_PEERS = "SELECT * FROM system.peers";

    private static final InetAddress bindAllAddress;

    static {
        try {
            bindAllAddress = InetAddress.getByAddress(new byte[4]);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private Cluster cluster;
    private ControlConnection controlConnection;

    @Override
    public void init(Cluster cluster) {
        checkNotNull(cluster);
        this.cluster = cluster;
        controlConnection = cluster.manager.controlConnection;
        checkNotNull(controlConnection);
    }

    @Override
    public Future<Set<HostInfo>> scan() throws DiscoveryException, ExecutionException, InterruptedException {
        checkNotNull(cluster, "You must call init() prior to calling this method");
        logger.debug("[Discovery policy] start scan");
        final Connection connection = borrowConnection();
        DefaultResultSetFuture peersFuture = allPeersResultSetFuture();
        ListenableFuture<Set<HostInfo>> scanFuture = Futures.transform(peersFuture, new Function<ResultSet, Set<HostInfo>>() {
            @Override
            public Set<HostInfo> apply(ResultSet input) {
                Set<HostInfo> peers = new HashSet<HostInfo>();
                for (Row peerRow : input) {
                    InetAddress peerAddress = addressToUseForPeer(connection, peerRow);
                    if (peerAddress == null)
                        continue;
                    HostInfo peer = hostInfoFromRow(peerRow);
                    peers.add(peer);
                }
                return peers;
            }
        });
        sendRequest(connection, peersFuture);
        return scanFuture;
    }

    @Override
    public Future<HostInfo> refreshNodeInfo(Host host) throws DiscoveryException, ExecutionException, InterruptedException {
        checkNotNull(cluster, "You must call init() prior to calling this method");
        checkNotNull(host);
        Connection connection = borrowConnection();
        Row row = fetchRowForHost(connection, host);
        if (row == null) {
            if (connection.isDefunct()) {
                logger.debug("Control connection is down, could not refresh node info");
                // Keep going with what we currently know about the node, otherwise we will ignore all nodes
                // until the control connection is back up (which leads to a catch-22 if there is only one)
                return Futures.immediateFuture(hostInfoFromHost(host));
            } else {
                logger.warn("No row found for host {} in {}'s peers system table. {} will be ignored.", host.getAddress(), connection.address, host.getAddress());
                return Futures.immediateFuture(null);
            }
        } else if (!connection.address.equals(host.getSocketAddress()) && row.getInet("rpc_address") == null) {
            logger.warn("No rpc_address found for host {} in {}'s peers system table. {} will be ignored.", host.getAddress(), connection.address, host.getAddress());
            return Futures.immediateFuture(null);
        }
        return Futures.immediateFuture(hostInfoFromHost(host));
    }

    @Override
    public void close() {
        this.cluster = null;
        this.controlConnection = null;
    }

    /**
     * @return the connection to use
     * @throws DiscoveryException
     */
    private Connection borrowConnection() {
        Connection connection = controlConnection.discoveryConnectionRef.get();
        if (connection != null && !connection.isClosed())
            return connection;
        throw new DiscoveryException("No connection available");
    }

    private ProtocolVersion getProtocolVersion() {
        return cluster.getConfiguration()
            .getProtocolOptions()
            .getProtocolVersionEnum();
    }

    private Row fetchRowForHost(Connection connection, Host peer) throws DiscoveryException, ExecutionException, InterruptedException {
        if (peer.listenAddress != null) {
            DefaultResultSetFuture future = singlePeerResultSetFuture(peer);
            sendRequest(connection, future);
            ResultSet rows = future.get();
            return rows.one();
        } else {
            // We have to fetch the whole peers table and find the host we're looking for
            DefaultResultSetFuture future = allPeersResultSetFuture();
            sendRequest(connection, future);
            ResultSet rows = future.get();
            for (Row row : rows) {
                InetAddress addr = addressToUseForPeer(connection, row);
                if (addr != null && addr.equals(peer.getAddress()))
                    return row;
            }
            return null;
        }
    }

    private DefaultResultSetFuture allPeersResultSetFuture() {
        return new DefaultResultSetFuture(null, getProtocolVersion(), new Requests.Query(SELECT_PEERS));
    }

    private DefaultResultSetFuture singlePeerResultSetFuture(Host peer) {
        String query = SELECT_PEERS + " WHERE peer='" + peer.listenAddress.getHostAddress() + '\'';
        return new DefaultResultSetFuture(null, getProtocolVersion(), new Requests.Query(query));
    }

    private void sendRequest(Connection connection, DefaultResultSetFuture future) throws DiscoveryException {
        try {
            connection.write(future);
        } catch (ConnectionException e) {
            throw new DiscoveryException(e);
        } catch (BusyConnectionException e) {
            throw new DiscoveryException(e);
        }
    }

    private static InetAddress addressToUseForPeer(Connection connection, Row peerRow) {
        InetAddress listenAddress = peerRow.getInet("peer");
        InetAddress rpcAddress = peerRow.getInet("rpc_address");
        if (Objects.equal(listenAddress, connection.address.getAddress()) || Objects.equal(rpcAddress, connection.address.getAddress())) {
            // Some DSE versions were inserting a line for the local node in peers (with mostly null values). This has been fixed, but if we
            // detect that's the case, ignore it as it's not really a big deal.
            logger.debug("System.peers on node {} has a line for itself. This is not normal but is a known problem of some DSE versions. Ignoring the entry.", connection.address);
            return null;
        } else if (rpcAddress == null) {
            // Ignore hosts with a null rpc_address, as this is most likely a phantom row in system.peers (JAVA-428).
            // Don't test this for the control host since we're already connected to it anyway, and we read the info from system.local
            // which doesn't have an rpc_address column (JAVA-546).
            logger.warn("No rpc_address found for host {} in {}'s peers system table. {} will be ignored.", listenAddress, connection.address, listenAddress);
            return null;
        } else if (rpcAddress.equals(bindAllAddress)) {
            logger.warn("Found host with 0.0.0.0 as rpc_address, using listen_address ({}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", listenAddress);
            rpcAddress = listenAddress;
        }
        return rpcAddress;
    }

    private static HostInfo hostInfoFromRow(Row peerRow) {
        return new HostInfo(
            peerRow.getInet("peer"),
            peerRow.getString("data_center"),
            peerRow.getString("rack"),
            VersionNumber.parse(peerRow.getString("release_version")),
            peerRow.getUUID("schema_version"),
            peerRow.getSet("tokens", String.class));
    }

    private static HostInfo hostInfoFromHost(Host host) {
        return new HostInfo(
            host.getAddress(),
            host.getDatacenter(),
            host.getRack(),
            host.getCassandraVersion(),
            null,
            Sets.newHashSet(Iterables.transform(host.getTokens(), Functions.toStringFunction())));
    }

}
