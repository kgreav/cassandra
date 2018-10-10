/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.cassandra.dht.RangeStreamer.FetchReplica;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.RackInferringSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.HostStatWithPort;
import org.apache.cassandra.tools.nodetool.SetHostStatWithPort;
import org.apache.cassandra.utils.FBUtilities;

@RunWith(OrderedJUnit4ClassRunner.class)
public class BootStrapperTest
{
    static IPartitioner oldPartitioner;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BootStrapperTest.class);

    static Predicate<Replica> originalAlivePredicate = RangeStreamer.ALIVE_PREDICATE;
    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.startGossiper();
        SchemaLoader.prepareServer();
        SchemaLoader.schemaDefinition("BootStrapperTest");
        RangeStreamer.ALIVE_PREDICATE = Predicates.alwaysTrue();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
        RangeStreamer.ALIVE_PREDICATE = originalAlivePredicate;
    }

    @Test
    public void testSourceTargetComputation() throws UnknownHostException
    {
        final int[] clusterSizes = new int[] { 1, 3, 5, 10, 100};
        for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces())
        {
            int replicationFactor = Keyspace.open(keyspaceName).getReplicationStrategy().getReplicationFactor().allReplicas;
            for (int clusterSize : clusterSizes)
                if (clusterSize >= replicationFactor)
                    testSourceTargetComputation(keyspaceName, clusterSize, replicationFactor);
        }
    }

    private RangeStreamer testSourceTargetComputation(String keyspaceName, int numOldNodes, int replicationFactor) throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();

        generateFakeEndpoints(numOldNodes);
        Token myToken = tmd.partitioner.getRandomToken();
        InetAddressAndPort myEndpoint = InetAddressAndPort.getByName("127.0.0.1");

        assertEquals(numOldNodes, tmd.sortedTokens().size());
        IFailureDetector mockFailureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddressAndPort ep)
            {
                return true;
            }

            public void interpret(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void report(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void remove(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void forceConviction(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
        };
        RangeStreamer s = new RangeStreamer(tmd, null, myEndpoint, StreamOperation.BOOTSTRAP, true, DatabaseDescriptor.getEndpointSnitch(), new StreamStateStore(), mockFailureDetector, false, 1);
        assertNotNull(Keyspace.open(keyspaceName));
        s.addRanges(keyspaceName, Keyspace.open(keyspaceName).getReplicationStrategy().getPendingAddressRanges(tmd, myToken, myEndpoint));


        Multimap<InetAddressAndPort, FetchReplica> toFetch = s.toFetch().get(keyspaceName);

        // Check we get get RF new ranges in total
        assertEquals(replicationFactor, toFetch.size());

        // there isn't any point in testing the size of these collections for any specific size.  When a random partitioner
        // is used, they will vary.
        assert toFetch.values().size() > 0;
        assert toFetch.keys().stream().noneMatch(myEndpoint::equals);
        return s;
    }

    private void generateFakeEndpoints(int numOldNodes) throws UnknownHostException
    {
        generateFakeEndpoints(StorageService.instance.getTokenMetadata(), numOldNodes, 1);
    }

    private void generateFakeEndpoints(TokenMetadata tmd, int numOldNodes, int numVNodes) throws UnknownHostException
    {
        tmd.clearUnsafe();
        generateFakeEndpoints(tmd, numOldNodes, numVNodes, "0", "0");
    }

    private void generateFakeEndpoints(TokenMetadata tmd, int numOldNodes, int numVNodes, String dc, String rack) throws UnknownHostException
    {
        IPartitioner p = tmd.partitioner;

        for (int i = 1; i <= numOldNodes; i++)
        {
            // leave .1 for myEndpoint
            InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + "." + rack + "." + (i + 1));
            List<Token> tokens = Lists.newArrayListWithCapacity(numVNodes);
            for (int j = 0; j < numVNodes; ++j)
                tokens.add(p.getRandomToken());
            
            tmd.updateNormalTokens(tokens, addr);
        }
    }
    
    @Test
    public void testAllocateTokens() throws UnknownHostException
    {
        int vn = 16;
        String ks = "BootStrapperTestKeyspace3";
        TokenMetadata tm = new TokenMetadata();
        generateFakeEndpoints(tm, 10, vn);
        InetAddressAndPort addr = FBUtilities.getBroadcastAddressAndPort();
        allocateTokensForNode(vn, ks, tm, addr);
    }

    public void testAllocateTokensNetworkStrategy(int rackCount, int replicas) throws UnknownHostException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
        try
        {
            DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
            int vn = 16;
            String ks = "BootStrapperTestNTSKeyspace" + rackCount + replicas;
            String dc = "1";

            // Register peers with expected DC for NetworkTopologyStrategy.
            TokenMetadata metadata = StorageService.instance.getTokenMetadata();
            metadata.clearUnsafe();
            metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.99"));
            metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.15.0.99"));

            SchemaLoader.createKeyspace(ks, KeyspaceParams.nts(dc, replicas, "15", 15), SchemaLoader.standardCFMD(ks, "Standard1"));
            TokenMetadata tm = StorageService.instance.getTokenMetadata();
            tm.clearUnsafe();
            for (int i = 0; i < rackCount; ++i)
                generateFakeEndpoints(tm, 10, vn, dc, Integer.toString(i));
            InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + ".0.99");
            allocateTokensForNode(vn, ks, tm, addr);
            // Note: Not matching replication factor in second datacentre, but this should not affect us.
        } finally {
            DatabaseDescriptor.setEndpointSnitch(oldSnitch);
        }
    }

    @Test
    public void testAllocateTokensNetworkStrategyOneRack() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(1, 3);
    }

    @Test(expected = ConfigurationException.class)
    public void testAllocateTokensNetworkStrategyTwoRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(2, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyThreeRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(3, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyFiveRacks() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(5, 3);
    }

    @Test
    public void testAllocateTokensNetworkStrategyOneRackOneReplica() throws UnknownHostException
    {
        testAllocateTokensNetworkStrategy(1, 1);
    }

    private void allocateTokensForNode(int vn, String ks, TokenMetadata tm, InetAddressAndPort addr)
    {
        SummaryStatistics os = TokenAllocation.replicatedOwnershipStats(tm.cloneOnlyTokenMap(), Keyspace.open(ks).getReplicationStrategy(), addr);
        Collection<Token> tokens = BootStrapper.allocateTokens(tm, addr, ks, vn, 0);
        assertEquals(vn, tokens.size());
        tm.updateNormalTokens(tokens, addr);
        SummaryStatistics ns = TokenAllocation.replicatedOwnershipStats(tm.cloneOnlyTokenMap(), Keyspace.open(ks).getReplicationStrategy(), addr);
        verifyImprovement(os, ns);
    }

    private void verifyImprovement(SummaryStatistics os, SummaryStatistics ns)
    {
        if (ns.getStandardDeviation() > os.getStandardDeviation())
        {
            fail(String.format("Token allocation unexpectedly increased standard deviation.\nStats before:\n%s\nStats after:\n%s", os, ns));
        }
    }

    
    @Test
    public void testAllocateTokensMultipleKeyspaces() throws UnknownHostException
    {
        // TODO: This scenario isn't supported very well. Investigate a multi-keyspace version of the algorithm.
        int vn = 16;
        String ks3 = "BootStrapperTestKeyspace4"; // RF = 3
        String ks2 = "BootStrapperTestKeyspace5"; // RF = 2

        TokenMetadata tm = new TokenMetadata();
        generateFakeEndpoints(tm, 10, vn);
        
        InetAddressAndPort dcaddr = FBUtilities.getBroadcastAddressAndPort();
        SummaryStatistics os3 = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks3).getReplicationStrategy(), dcaddr);
        SummaryStatistics os2 = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks2).getReplicationStrategy(), dcaddr);
        String cks = ks3;
        String nks = ks2;
        for (int i=11; i<=20; ++i)
        {
            allocateTokensForNode(vn, cks, tm, InetAddressAndPort.getByName("127.0.0." + (i + 1)));
            String t = cks; cks = nks; nks = t;
        }
        
        SummaryStatistics ns3 = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks3).getReplicationStrategy(), dcaddr);
        SummaryStatistics ns2 = TokenAllocation.replicatedOwnershipStats(tm, Keyspace.open(ks2).getReplicationStrategy(), dcaddr);
        verifyImprovement(os3, ns3);
        verifyImprovement(os2, ns2);
    }


    @Test
    public void testBasic() throws UnknownHostException, FileNotFoundException
    {

        for (double i = 1; i < 7; ++i)
        {
//            // 1 rack, RF = 3
//            testNTS(1, 3, (int) Math.pow(2, i), 4, 3, 1);
//            testNTS(1, 3, (int) Math.pow(2, i), 8, 3, 1);
//            testNTS(1, 3, (int) Math.pow(2, i), 16, 3, 1);
//            testNTS(1, 3, (int) Math.pow(2, i), 32, 3, 1);
//            testNTS(1, 3, (int) Math.pow(2, i), 64, 3, 1);
//
//            // racks == RF == 3
//            testNTS(3, 3, (int) Math.pow(2, i), 4, 3, 1);
//            testNTS(3, 3, (int) Math.pow(2, i), 8, 3, 1);
//            testNTS(3, 3, (int) Math.pow(2, i), 16, 3, 1);
            testNTS(3, 3, (int) Math.pow(2, i), 32, 3, 1);
            testNTS(3, 3, (int) Math.pow(2, i), 64, 3, 1);

            // racks = 5, RF = 3
//            testNTS(5, 3, (int) Math.pow(2, i), 4, 3, 1);
//            testNTS(5, 3, (int) Math.pow(2, i), 8, 3, 1);
            testNTS(5, 3, (int) Math.pow(2, i), 16, 3, 1);
            testNTS(5, 3, (int) Math.pow(2, i), 32, 3, 1);
            testNTS(5, 3, (int) Math.pow(2, i), 64, 3, 1);

//            // RF = 5, racks = 1
//            testNTS(1, 5, (int) Math.pow(2, i), 4, 3, 1);
//            testNTS(1, 5, (int) Math.pow(2, i), 8, 3, 1);
//            testNTS(1, 5, (int) Math.pow(2, i), 16, 3, 1);
//            testNTS(1, 5, (int) Math.pow(2, i), 32, 3, 1);
//            testNTS(1, 5, (int) Math.pow(2, i), 64, 3, 1);

            // RF = 5, racks = 5
            testNTS(5, 5, (int) Math.pow(2, i), 4, 5, 1);
            testNTS(5, 5, (int) Math.pow(2, i), 8, 5, 1);
            testNTS(5, 5, (int) Math.pow(2, i), 16, 5, 1);
            testNTS(5, 5, (int) Math.pow(2, i), 32, 5, 1);
            testNTS(5, 5, (int) Math.pow(2, i), 64, 5, 1);

        }

        for (double i = 1; i < 6; i++)
        {
            // 1 rack, RF 3 in 2 DC's
            testNTS(1, 3, (int) Math.pow(2, i), 4, 3, 2);
            testNTS(1, 3, (int) Math.pow(2, i), 8, 3, 2);
            testNTS(1, 3, (int) Math.pow(2, i), 16, 3, 2);
            testNTS(1, 3, (int) Math.pow(2, i), 32, 3, 2);
            testNTS(1, 3, (int) Math.pow(2, i), 64, 3, 2);
        }

    }

    public void testNTS(int rackCount, int replicas, int nodesPerRack, int vNodes, int seedCount, float secondDC) throws UnknownHostException, FileNotFoundException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
        try
        {
            DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
            String ks = "BootStrapperTestNTSKeyspace" + rackCount + "_" + replicas + "_" + nodesPerRack + "_" + vNodes + "_" + (int) Math.ceil(secondDC) ;
            String dc = "1";

            // Register peers with expected DC for NetworkTopologyStrategy.
            TokenMetadata metadata = StorageService.instance.getTokenMetadata();
            metadata.clearUnsafe();
            metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.99"));

            KeyspaceParams kp = KeyspaceParams.nts(dc, replicas);

            SchemaLoader.createKeyspace(ks, kp, SchemaLoader.standardCFMD(ks, "Standard1"));
            TokenMetadata tm = StorageService.instance.getTokenMetadata();
            tm.clearUnsafe();
            // Create seed nodes. 1 per rack, max of #racks
            for (int i = 0; i < seedCount && i < rackCount; ++i)
            {
                generateFakeEndpoints(tm, 1, vNodes, dc, Integer.toString(i));
            }
            String numDC = secondDC > 1.0 ? "multiDC" : "singleDC";
            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream("/home/kurt/vnode_testing/" + rackCount + "racks_" + replicas + "rf_" + rackCount*nodesPerRack + "nodes_" + vNodes + "vNodes_" + (seedCount % rackCount + 1) + "seeds_" + numDC)))))
            {
                InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + ".0.99");
                SummaryStatistics os = TokenAllocation.replicatedOwnershipStats(tm.cloneOnlyTokenMap(), Keyspace.open(ks).getReplicationStrategy(), addr);
                out.write(String.format("%s, %s, %s\n", os.getN(), os.getVariance(), os.getStandardDeviation()));
                int totalNodes = 1;
                for (int j = 0; j < rackCount; ++j)
                {
                    if (totalNodes > (nodesPerRack * rackCount + seedCount) / secondDC)
                        dc = "2";
                    // Skip i = 0 to reserve for possible seeds
                    for (int i = 3; i < nodesPerRack + 3; ++i)
                    {
                        totalNodes++;
                        if (rackCount == 1 && totalNodes > (nodesPerRack + seedCount) / secondDC)
                        {
                            dc = "2";
                            addr = InetAddressAndPort.getByName("127." + dc + "." + j + "." + i);
                            allocateTokens(vNodes, ks, tm, addr, out);

                            kp = KeyspaceParams.nts("1", replicas, "2", replicas);
                            MigrationManager.announceKeyspaceUpdate(KeyspaceMetadata.create(ks, kp));
                            continue;
                        }
                        addr = InetAddressAndPort.getByName("127." + dc + "." + j + "." + i);
                        try
                        {
                            allocateTokens(vNodes, ks, tm, addr, out);
                        } catch (ConfigurationException e)
                        {
                            logger.error("SHITS BROKEN", e);
                            allocateTokens(vNodes, ks, tm, addr, out);
                        }
                    }
                }
                LinkedHashMap<String, Float> ownerships = StorageService.instance.effectiveOwnershipWithPort(ks);
                SortedMap<String, SetHostStatWithPort> dcs = getOwnershipByDcWithPort(false, StorageService.instance.getTokenToEndpointWithPortMap(), ownerships);

                // PRint out the shit Datacenters
                for (Map.Entry<String, SetHostStatWithPort> datac : dcs.entrySet())
                {
                    String dcHeader = String.format("Datacenter: %s%n", datac.getKey());
                    out.write(dcHeader);


                    ArrayListMultimap<InetAddressAndPort, HostStatWithPort> hostToTokens = ArrayListMultimap.create();
                    for (HostStatWithPort stat : datac.getValue())
                        hostToTokens.put(stat.endpoint, stat);

                    for (InetAddressAndPort endpoint : hostToTokens.keySet())
                    {
                        Float owns = ownerships.get(endpoint.toString());
                        out.println(new DecimalFormat("##0.0%").format(owns));
                    }
                }
            }
            // Note: Not matching replication factor in second datacentre, but this should not affect us.
        } finally {
            DatabaseDescriptor.setEndpointSnitch(oldSnitch);
        }
    }

    private void allocateTokens(int vn, String ks, TokenMetadata tm, InetAddressAndPort addr, PrintWriter s)
    {
        Collection<Token> tokens = BootStrapper.allocateTokens(tm, addr, ks, vn, 0);
        assertEquals(vn, tokens.size());
        tm.updateNormalTokens(tokens, addr);
        SummaryStatistics ns = TokenAllocation.replicatedOwnershipStats(tm.cloneOnlyTokenMap(), Keyspace.open(ks).getReplicationStrategy(), addr);
        ns.getVariance();
        ns.getStandardDeviation();
        s.write(String.format("%s, %s, %s\n", ns.getN(), ns.getVariance(), ns.getStandardDeviation()));
    }

    private SortedMap<String, SetHostStatWithPort> getOwnershipByDcWithPort(boolean resolveIp,
                                                                      Map<String, String> tokenToEndpoint,
                                                                      Map<String, Float> ownerships)
    {
        SortedMap<String, SetHostStatWithPort> ownershipByDc = Maps.newTreeMap();
        try
        {
            for (Map.Entry<String, String> tokenAndEndPoint : tokenToEndpoint.entrySet())
            {
                String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(InetAddressAndPort.getByName(tokenAndEndPoint.getValue()));
                if (!ownershipByDc.containsKey(dc))
                    ownershipByDc.put(dc, new SetHostStatWithPort(resolveIp));
                ownershipByDc.get(dc).add(tokenAndEndPoint.getKey(), tokenAndEndPoint.getValue(), ownerships);
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        return ownershipByDc;
    }

}
