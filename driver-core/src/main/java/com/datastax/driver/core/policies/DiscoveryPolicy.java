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
package com.datastax.driver.core.policies;

import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.exceptions.DiscoveryException;

/**
 * Defines a strategy to discover peers in the cluster and information about it,
 * such as its name, the partitioner in use, token maps and schema versions.
 */
public interface DiscoveryPolicy {

    class HostInfo {

        /**
         * The datacenter the host belongs to.
         * If null, load balancing policy will not be notified of this host
         */
        private final String datacenter;

        /**
         * The rack the host belongs to.
         * If null, load balancing policy will not be notified of this host
         */
        private final String rack;

        /**
         * The host version of Cassandra.
         */
        private final VersionNumber cassandraVersion;

        /**
         * The address the host listens to for incoming client requests.
         * Should be {@code rpc_address} in Cassandra's config file.
         */
        private final InetAddress listenAddress;

        private final Set<String> tokens;

        private final UUID schemaVersion;

        public HostInfo(InetAddress listenAddress, String datacenter, String rack, VersionNumber cassandraVersion, UUID schemaVersion, Set<String> tokens) {
            this.datacenter = datacenter;
            this.rack = rack;
            this.cassandraVersion = cassandraVersion;
            this.listenAddress = listenAddress;
            this.schemaVersion = schemaVersion;
            this.tokens = tokens;
        }

        public InetAddress getListenAddress() {
            return listenAddress;
        }

        public String getDatacenter() {
            return datacenter;
        }

        public String getRack() {
            return rack;
        }

        public VersionNumber getCassandraVersion() {
            return cassandraVersion;
        }

        public Set<String> getTokens() {
            return tokens;
        }

        public UUID getSchemaVersion() {
            return schemaVersion;
        }
    }

    /**
     * Called at cluster initialization.
     *
     * @param cluster the cluster whose peers are to be discovered.
     */
    void init(Cluster cluster);

    /**
     * Perform a full scan of the cluster and return all the nodes found.
     *
     * @return discovered nodes
     * @throws DiscoveryException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    Future<Set<HostInfo>> scan() throws DiscoveryException, ExecutionException, InterruptedException;

    /**
     * Refresh information for the given host.
     * Return null, or info without listen address means host ignored by control connection.
     *
     * @param host the host to refresh.
     * @return the updated host info
     * @throws DiscoveryException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    Future<HostInfo> refreshNodeInfo(Host host) throws DiscoveryException, ExecutionException, InterruptedException;

    /**
     * Called on cluster close.
     */
    void close();

}
