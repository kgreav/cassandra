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
package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.SimpleBuilders.PartitionUpdateBuilder;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;

public class BatchlogManager implements BatchlogManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final long REPLAY_INTERVAL = 10 * 1000; // milliseconds
    static final int DEFAULT_PAGE_SIZE = 128;

    // Batches older than this are removed. Note we could probably use a default TTL to achieve the same behaviour,
    // but we'd have no way to control half expired batches from being replayed
    static final long MAX_BATCH_AGE = TimeUnit.MINUTES.toMillis(30);
    static final int MAX_MUTATION_SIZE = DatabaseDescriptor.getMaxMutationSize() / 2;

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();
    public static final long BATCHLOG_REPLAY_TIMEOUT = Long.getLong("cassandra.batchlog.replay_timeout_in_ms", DatabaseDescriptor.getWriteRpcTimeout() * 2);

    private volatile long totalBatchesReplayed = 0; // no concurrency protection necessary as only written by replay thread.
    private volatile UUID firstInactiveBatch = UUIDGen.minTimeUUID(0);

    // Single-thread executor service for scheduling and serializing log replay.
    private final ScheduledExecutorService batchlogTasks;

    private final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);

    public BatchlogManager()
    {
        ScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("BatchlogTasks");
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        batchlogTasks = executor;
    }

    public void start()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        batchlogTasks.scheduleWithFixedDelay(this::replayFailedBatches,
                                             StorageService.RING_DELAY,
                                             REPLAY_INTERVAL,
                                             TimeUnit.MILLISECONDS);
    }

    public void shutdown() throws InterruptedException
    {
        batchlogTasks.shutdown();
        batchlogTasks.awaitTermination(60, TimeUnit.SECONDS);
    }

    /**
     * Deletes a batch from the batchlog
     * @param id Batch ID to delete
     */
    public static void remove(UUID id)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batches,
                                                         UUIDType.instance.decompose(id),
                                                         FBUtilities.timestampMicros(),
                                                         FBUtilities.nowInSeconds()))
            .apply();
    }

    public static void store(Batch batch)
    {
        store(batch, true, true);
    }

    public static void store(Batch batch, boolean durableWrites)
    {
        store(batch, true, true);
    }

    /**
     * Stores a batch to the batchlog. Batches must complete storing within MAX_BATCH_AGE, otherwise they will
     * be deleted without replayment. See {@link BatchlogManager#processBatchlogEntries(UntypedResultSet, int)}}
     * @param batch Batch to store in the batchlog. See {@link SystemKeyspace#Batches} for how a batch is stored.
     * @param durableWrites Whether the storing of this batch is also applied to the commitlog
     * @param markActive Whether to mark this batch active at the completion of storing
     * TODO: performance testing if chunking mutations is necessary
     */
    public static void store(Batch batch, boolean durableWrites, boolean markActive)
    {
        // We store active first so that if this batch gets replayed while still being written
        // we can check against active to avoid replaying incomplete batches.
        PartitionUpdateBuilder builder = new PartitionUpdateBuilder(SystemKeyspace.Batches, batch.id);
        builder.row(Clustering.STATIC_CLUSTERING)
               .timestamp(batch.creationTime)
               .add("version", MessagingService.current_version)
               .add("active", false);
        builder.buildAsMutation().apply(durableWrites);

        long index = 0;
//        int mutationSize = 0;
        boolean needsApply = false;
        for (ByteBuffer mutation : batch.encodedMutations)
        {
            needsApply = true;
//            mutationSize += mutation.remaining();
            builder = new PartitionUpdateBuilder(SystemKeyspace.Batches, batch.id);
            builder.row(index++)
                   .timestamp(batch.creationTime)
                   .add("mutation", mutation);
            builder.buildAsMutation().apply(durableWrites);
//            if (mutationSize > MAX_MUTATION_SIZE)
//            {
//                builder.buildAsMutation().apply(durableWrites);
//                mutationSize = 0;
//                needsApply = false;
//            }
        }

        for (Mutation mutation : batch.decodedMutations)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                needsApply = true;
//                mutationSize += buffer.getLength();
                Mutation.serializer.serialize(mutation, buffer, MessagingService.current_version);
                builder = new PartitionUpdateBuilder(SystemKeyspace.Batches, batch.id);
                builder.row(index++)
                       .timestamp(batch.creationTime)
                       .add("mutation", buffer.buffer());
                builder.buildAsMutation().apply(durableWrites);
//                if (mutationSize > MAX_MUTATION_SIZE)
//                {
//                    builder.buildAsMutation().apply(durableWrites);
//                    mutationSize = 0;
//                    needsApply = false;
//                }
            }
            catch (IOException e)
            {
                // shouldn't happen
                throw new AssertionError(e);
            }
        }

        builder.buildAsMutation().apply(durableWrites);
        // If not made active batch will expire after MAX_BATCH_AGE
        if (markActive)
        {
            // make batch active
            builder = new PartitionUpdateBuilder(SystemKeyspace.Batches, batch.id);
            builder.row(Clustering.STATIC_CLUSTERING)
                   .timestamp(batch.creationTime)
                   .add("active", true);
            needsApply = true;
        }

        if (needsApply)
            builder.buildAsMutation().apply(durableWrites);
    }

    @VisibleForTesting
    public int countAllBatches()
    {
        String query = String.format("SELECT DISTINCT batch_id FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES);
        UntypedResultSet results = executeInternal(query);
        if (results == null || results.isEmpty())
            return 0;

        return results.size();
    }

    /**
     * Marks specified batch to be active and ready to be replayed.
     * Should only be called after {@link #store(Batch)}.
     * NOTE if {@code id} doesn't exist in the batchlog we will create it, and thus {@code id} will be an "active" batch
     * with no mutations. We protect against this in {@link #processBatchlogEntries(UntypedResultSet, int)}.
     * @param id UUID of batch previously stored in the batchlog
     * @param durableWrites True to ensure activation persists through commitlog
     */
    public static void markBatchActive(UUID id, boolean durableWrites)
    {
        // make batch active
        PartitionUpdateBuilder builder = new PartitionUpdateBuilder(SystemKeyspace.Batches, id);
        builder.row(Clustering.STATIC_CLUSTERING)
               .add("active", true);
        builder.buildAsMutation().apply(durableWrites);
    }

    /**
     * Mark given batch IDs to be active and ready to be replayed.
     * Should only be called after {@link #store(Batch)}.
     * NOTE if any of {@code ids} doesn't exist in the batchlog we will create it, and thus {@code id} will be an
     * "active" batch with no mutations. We protect against this in {@link #processBatchlogEntries(UntypedResultSet, int)}.
     * @param ids UUIDs of batches previously stored in the batchlog
     * @param durableWrites True to ensure activation persists through commitlog
     * TODO: Should this use the batchlog? - currently cannot use batchlog because batchlog tables use GCGS = 0,
     * and {@link org.apache.cassandra.batchlog.BatchlogManager.ReplayingBatch#replay(com.google.common.util.concurrent.RateLimiter, java.util.Set)}
     * will use GCGS to determine if it's worth replaying (for some reason?)
     */
    public static void markBatchesActive(List<UUID> ids, boolean durableWrites)
    {
        logger.trace("Marking {} batches as active", ids.size());
        for (UUID id : ids)
        {
            PartitionUpdateBuilder builder = new PartitionUpdateBuilder(SystemKeyspace.Batches, id);
            builder.row(Clustering.STATIC_CLUSTERING)
                   .add("active", true);
            builder.buildAsMutation().apply(durableWrites);
        }

        // We use the batchlog to ensure that all batches are eventually marked active. This is important for
        // MVs, as we need to make sure that if we are splitting a MV's updates over multiple batches all of them
        // need to be applied to guarantee consistency between base and view.
//        store(Batch.createLocal(UUIDGen.getTimeUUID(System.currentTimeMillis() - getBatchlogTimeout()),
//                                FBUtilities.timestampMicros(),
//                                mutations),
//              durableWrites);

    }

    public long getTotalBatchesReplayed()
    {
        return totalBatchesReplayed;
    }

    public void forceBatchlogReplay() throws Exception
    {
        startBatchlogReplay().get();
    }

    public Future<?> startBatchlogReplay()
    {
        // If a replay is already in progress this request will be executed after it completes.
        return batchlogTasks.submit(this::replayFailedBatches);
    }

    void performInitialReplay() throws InterruptedException, ExecutionException
    {
        // Invokes initial replay. Used for testing only.
        batchlogTasks.submit(this::replayFailedBatches).get();
    }

    private void replayFailedBatches()
    {
        logger.trace("Started replayFailedBatches");

        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount <= 0)
        {
            logger.trace("Replay cancelled as there are no peers in the ring.");
            return;
        }

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
        setRate(DatabaseDescriptor.getBatchlogReplayThrottleInKB());

        UUID currentTimeUUID = UUIDGen.maxTimeUUID(System.currentTimeMillis() - getBatchlogTimeout());
        ColumnFamilyStore store = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES);
        int pageSize = calculatePageSize(store);
        // At every replay we store the UUID of the first inactive batch seen, since it could become active in the next
        // replay. Since every processed active batch is deleted, tombstone content may still be present in the
        // tables, so we specify token(id) >= token(firstInactiveBatch) as part of the query to avoid walking over
        // tombstones before the first inactive seen batch.
        String query = String.format("SELECT batch_id, mutation, version, active FROM %s.%s WHERE token(batch_id) >= token(?) AND token(batch_id) <= token(?)",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.BATCHES);
        UntypedResultSet batches = executeInternalWithPaging(query, pageSize, firstInactiveBatch, currentTimeUUID);
        processBatchlogEntries(batches, pageSize);
        logger.trace("Finished replayFailedBatches");
    }

    /**
     * Sets the rate for the current rate limiter. When {@code throttleInKB} is 0, this sets the rate to
     * {@link Double#MAX_VALUE} bytes per second.
     *
     * @param throttleInKB throughput to set in KB per second
     */
    public void setRate(final int throttleInKB)
    {
        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount > 0)
        {
            int endpointThrottleInKB = throttleInKB / endpointsCount;
            double throughput = endpointThrottleInKB == 0 ? Double.MAX_VALUE : endpointThrottleInKB * 1024.0;
            if (rateLimiter.getRate() != throughput)
            {
                logger.debug("Updating batchlog replay throttle to {} KB/s, {} KB/s per endpoint", throttleInKB, endpointThrottleInKB);
                rateLimiter.setRate(throughput);
            }
        }
    }

    // read less rows (batches) per page if they are very large
    static int calculatePageSize(ColumnFamilyStore store)
    {
        double averageRowSize = store.getMeanPartitionSize();
        if (averageRowSize <= 0)
            return DEFAULT_PAGE_SIZE;

        return (int) Math.max(1, Math.min(DEFAULT_PAGE_SIZE, 4 * 1024 * 1024 / averageRowSize));
    }

    /**
     * Replays active batches, removing any older than {@code MAX_BATCH_AGE}. Will track the earliest inactive batch by
     * setting {@code firstInactiveBatch}. Should only be called as per {@link #replayFailedBatches()}
     * @param batches Set of results from the batchlog.
     * @param pageSize Number of batches in page of results. Used to optimise amount of flushing
     */
    private void processBatchlogEntries(UntypedResultSet batches, int pageSize)
    {
        long positionInPage = 0;
        ArrayList<ReplayingBatch> unfinishedBatches = new ArrayList<>(pageSize);

        Set<InetAddress> hintedNodes = new HashSet<>();
        Set<UUID> replayedBatches = new HashSet<>();
        ByteBuffer EXPIRED_BATCH_ID = TimeUUIDType.instance.decompose(
            UUIDGen.getTimeUUID(System.currentTimeMillis() - MAX_BATCH_AGE)
        );

        UUID currentId, lastId;
        currentId = UUIDGen.getTimeUUID(Long.MIN_VALUE);
        int currentVersion = -1;
        ReplayingBatch batch = null;
        for (UntypedResultSet.Row row : batches)
        {
            positionInPage++;
            lastId = currentId;
            currentId = row.getUUID("batch_id");

            if (!lastId.equals(currentId)) // start of new batch
            {
                // first, replay previous batch if it exists
                if (batch != null)
                {
                    if (!replay(batch, unfinishedBatches, hintedNodes))
                    {
                        batch = null;
                        continue;
                    }
                }

                // This is to catch batches that will never become active due to C* crashing before completing the batch.
                // This creates a side effect: If a batch takes longer than MAX_BATCH_AGE to write, it will be removed.
                // Note that we use TimeUUIDType compare here because UUID.compare doesn't recognise time ordered UUIDs.
                if (TimeUUIDType.instance.compare(TimeUUIDType.instance.decompose(currentId), EXPIRED_BATCH_ID) < 0)
                {
                    logger.warn("Removing expired incomplete batch {}.", currentId);
                    remove(currentId);
                    batch = null; // skip over rest of this batch
                    continue;
                }

                // An active row should always have a mutation, however to deal with garbage in the batchlog we only
                // try to replay an active batch that has mutations. markBatchActive() could potentially create
                // rows with no mutations.
                if (row.getBoolean("active"))
                {
                    if (row.has("mutation"))
                    {
                        // Set up batch for current row
                        currentVersion = row.getInt("version");
                        batch = new ReplayingBatch(currentId);
                    }
                    else
                    {
                        remove(currentId);
                        batch = null;
                    }
                }
                else
                {
                    if (currentId.compareTo(firstInactiveBatch) < 0)
                        firstInactiveBatch = currentId;
                    // not active, skip.
                    batch = null;
                }
            }

            if (batch == null) // skip to the next batch
            {
                continue;
            }
            else
            {
                try
                {
                    batch.addMutation(row.getBytes("mutation"), currentVersion);
                }
                catch (IOException e)
                {
                    // If a mutation is bad we will just remove and skip this whole batch
                    batch = null;
                    logger.error("Skipping batch {} due to {}", currentId, e.getMessage());
                    remove(currentId);
                }
            }

            if (positionInPage >= pageSize)
            {
                // We have reached the end of a page. To avoid keeping too many mutations in memory, finish processing
                // the page before requesting the next row.
                finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
                positionInPage = 0;
            }
        }
        // replay the last batch if it was active
        if (batch != null)
            replay(batch, unfinishedBatches, hintedNodes); // don't care about errors here

        logger.debug("Total batches replayed: {}", getTotalBatchesReplayed());
        finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);

        // to preserve batch guarantees, we must ensure that hints (if any) have made it to disk, before deleting the batches
        HintsService.instance.flushAndFsyncBlockingly(transform(hintedNodes, StorageService.instance::getHostIdForEndpoint));

        // once all generated hints are fsynced, actually delete the batches
        replayedBatches.forEach(BatchlogManager::remove);
    }

    /**
     * Replay a given batch. Will remove batches from the batchlog that either succeed by being applied locally, or where
     * no mutations were replayed, or where replaying fails.
     * @param batch Batch to be replayed
     * @param unfinishedBatches If batch doesn't finish, adds it to this list. Needs to be
     * @param hintedNodes Nodes to create hints for if they are down.
     * @return True if batch was replayable. Either replayed successfully, added to unfi
     */
    private boolean replay(ReplayingBatch batch, List<ReplayingBatch> unfinishedBatches, Set<InetAddress> hintedNodes)
    {
        try
        {
            if (batch.replay(rateLimiter, hintedNodes) > 0)
            {
                unfinishedBatches.add(batch);
            }
            else
            {
                // All writes were applied locally OR there were no mutations for this batch (because expired OR truncated CFs)
                remove(batch.id);
                ++totalBatchesReplayed;
            }
        }
        catch (IOException e)
        {
            logger.error("Failed batch replay of {} due to {}", batch.id, e.getMessage());
            //skip remaining mutations and remove batch
            remove(batch.id);
            return false;
        }
        return true;
    }

    private void finishAndClearBatches(ArrayList<ReplayingBatch> batches, Set<InetAddress> hintedNodes, Set<UUID> replayedBatches)
    {
        // schedule hints for timed out deliveries
        for (ReplayingBatch batch : batches)
        {
            batch.finish(hintedNodes);
            replayedBatches.add(batch.id);
        }

        totalBatchesReplayed += batches.size();
        batches.clear();
    }

    public static long getBatchlogTimeout()
    {
        return BATCHLOG_REPLAY_TIMEOUT; // enough time for the actual write + BM removal mutation
    }

    private static class ReplayingBatch
    {
        private final UUID id;
        private final long writtenAt;
        private final List<Mutation> mutations;
        private int replayedBytes;

        private List<ReplayWriteResponseHandler<Mutation>> replayHandlers;

        ReplayingBatch(UUID id)
        {
            this.id = id;
            this.writtenAt = UUIDGen.unixTimestamp(id);
            this.mutations = new ArrayList<>();
            this.replayedBytes = 0;
        }

        ReplayingBatch(UUID id, int version, List<ByteBuffer> serializedMutations) throws IOException
        {
            this.id = id;
            this.writtenAt = UUIDGen.unixTimestamp(id);
            this.mutations = new ArrayList<>(serializedMutations.size());
            this.replayedBytes = addMutations(version, serializedMutations);
        }

        public int replay(RateLimiter rateLimiter, Set<InetAddress> hintedNodes) throws IOException
        {
            logger.trace("Replaying batch {}", id);

            if (mutations.isEmpty())
                return 0;

            int gcgs = gcgs(mutations);
            if (TimeUnit.MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return 0;

            replayHandlers = sendReplays(mutations, writtenAt, hintedNodes);

            rateLimiter.acquire(replayedBytes); // acquire afterwards, to not mess up ttl calculation.

            return replayHandlers.size();
        }

        public void finish(Set<InetAddress> hintedNodes)
        {
            for (int i = 0; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                try
                {
                    handler.get();
                }
                catch (WriteTimeoutException|WriteFailureException e)
                {
                    logger.trace("Failed replaying a batched mutation to a node, will write a hint");
                    logger.trace("Failure was : {}", e.getMessage());
                    // writing hints for the rest to hints, starting from i
                    writeHintsForUndeliveredEndpoints(i, hintedNodes);
                    return;
                }
            }
        }

        private int addMutations(int version, List<ByteBuffer> serializedMutations) throws IOException
        {
            int ret = 0;
            for (ByteBuffer serializedMutation : serializedMutations)
            {
                ret += serializedMutation.remaining();
                try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
                {
                    addMutation(Mutation.serializer.deserialize(in, version));
                }
            }

            return ret;
        }

        // Remove CFs that have been truncated since. writtenAt and SystemTable#getTruncatedAt() both return millis.
        // We don't abort the replay entirely b/c this can be considered a success (truncated is same as delivered then
        // truncated.
        private void addMutation(Mutation mutation)
        {
            for (TableId tableId : mutation.getTableIds())
                if (writtenAt <= SystemKeyspace.getTruncatedAt(tableId))
                    mutation = mutation.without(tableId);

            if (!mutation.isEmpty())
                mutations.add(mutation);
        }

        private void addMutation(ByteBuffer serializedMutation, int version) throws IOException
        {
            replayedBytes += serializedMutation.remaining();
            try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
            {
                addMutation(Mutation.serializer.deserialize(in, version));
            }

        }

        private void writeHintsForUndeliveredEndpoints(int startFrom, Set<InetAddress> hintedNodes)
        {
            int gcgs = gcgs(mutations);

            // expired
            if (TimeUnit.MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return;

            for (int i = startFrom; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                Mutation undeliveredMutation = mutations.get(i);

                if (handler != null)
                {
                    hintedNodes.addAll(handler.undelivered);
                    HintsService.instance.write(transform(handler.undelivered, StorageService.instance::getHostIdForEndpoint),
                                                Hint.create(undeliveredMutation, writtenAt));
                }
            }
        }

        private static List<ReplayWriteResponseHandler<Mutation>> sendReplays(List<Mutation> mutations,
                                                                              long writtenAt,
                                                                              Set<InetAddress> hintedNodes)
        {
            List<ReplayWriteResponseHandler<Mutation>> handlers = new ArrayList<>(mutations.size());
            for (Mutation mutation : mutations)
            {
                ReplayWriteResponseHandler<Mutation> handler = sendSingleReplayMutation(mutation, writtenAt, hintedNodes);
                if (handler != null)
                    handlers.add(handler);
            }
            return handlers;
        }

        /**
         * We try to deliver the mutations to the replicas ourselves if they are alive and only resort to writing hints
         * when a replica is down or a write request times out.
         *
         * @return direct delivery handler to wait on or null, if no live nodes found
         */
        private static ReplayWriteResponseHandler<Mutation> sendSingleReplayMutation(final Mutation mutation,
                                                                                     long writtenAt,
                                                                                     Set<InetAddress> hintedNodes)
        {
            Set<InetAddress> liveEndpoints = new HashSet<>();
            String ks = mutation.getKeyspaceName();
            Token tk = mutation.key().getToken();

            for (InetAddress endpoint : StorageService.instance.getNaturalAndPendingEndpoints(ks, tk))
            {
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                {
                    mutation.apply();
                }
                else if (FailureDetector.instance.isAlive(endpoint))
                {
                    liveEndpoints.add(endpoint); // will try delivering directly instead of writing a hint.
                }
                else
                {
                    hintedNodes.add(endpoint);
                    HintsService.instance.write(StorageService.instance.getHostIdForEndpoint(endpoint),
                                                Hint.create(mutation, writtenAt));
                }
            }

            if (liveEndpoints.isEmpty())
                return null;

            ReplayWriteResponseHandler<Mutation> handler = new ReplayWriteResponseHandler<>(liveEndpoints, System.nanoTime());
            MessageOut<Mutation> message = mutation.createMessage();
            for (InetAddress endpoint : liveEndpoints)
                MessagingService.instance().sendRR(message, endpoint, handler, false);
            return handler;
        }

        private static int gcgs(Collection<Mutation> mutations)
        {
            int gcgs = Integer.MAX_VALUE;
            for (Mutation mutation : mutations)
                gcgs = Math.min(gcgs, mutation.smallestGCGS());
            return gcgs;
        }

        /**
         * A wrapper of WriteResponseHandler that stores the addresses of the endpoints from
         * which we did not receive a successful reply.
         */
        private static class ReplayWriteResponseHandler<T> extends WriteResponseHandler<T>
        {
            private final Set<InetAddress> undelivered = Collections.newSetFromMap(new ConcurrentHashMap<>());

            ReplayWriteResponseHandler(Collection<InetAddress> writeEndpoints, long queryStartNanoTime)
            {
                super(writeEndpoints, Collections.<InetAddress>emptySet(), null, null, null, WriteType.UNLOGGED_BATCH, queryStartNanoTime);
                undelivered.addAll(writeEndpoints);
            }

            @Override
            protected int totalBlockFor()
            {
                return this.naturalEndpoints.size();
            }

            @Override
            public void response(MessageIn<T> m)
            {
                boolean removed = undelivered.remove(m == null ? FBUtilities.getBroadcastAddress() : m.from);
                assert removed;
                super.response(m);
            }
        }
    }

    public static class EndpointFilter
    {
        private final String localRack;
        private final Multimap<String, InetAddress> endpoints;

        public EndpointFilter(String localRack, Multimap<String, InetAddress> endpoints)
        {
            this.localRack = localRack;
            this.endpoints = endpoints;
        }

        /**
         * @return list of candidates for batchlog hosting. If possible these will be two nodes from different racks.
         */
        public Collection<InetAddress> filter()
        {
            // special case for single-node data centers
            if (endpoints.values().size() == 1)
                return endpoints.values();

            // strip out dead endpoints and localhost
            ListMultimap<String, InetAddress> validated = ArrayListMultimap.create();
            for (Map.Entry<String, InetAddress> entry : endpoints.entries())
                if (isValid(entry.getValue()))
                    validated.put(entry.getKey(), entry.getValue());

            if (validated.size() <= 2)
                return validated.values();

            if (validated.size() - validated.get(localRack).size() >= 2)
            {
                // we have enough endpoints in other racks
                validated.removeAll(localRack);
            }

            if (validated.keySet().size() == 1)
            {
                /*
                 * we have only 1 `other` rack to select replicas from (whether it be the local rack or a single non-local rack)
                 * pick two random nodes from there; we are guaranteed to have at least two nodes in the single remaining rack
                 * because of the preceding if block.
                 */
                List<InetAddress> otherRack = Lists.newArrayList(validated.values());
                shuffle(otherRack);
                return otherRack.subList(0, 2);
            }

            // randomize which racks we pick from if more than 2 remaining
            Collection<String> racks;
            if (validated.keySet().size() == 2)
            {
                racks = validated.keySet();
            }
            else
            {
                racks = Lists.newArrayList(validated.keySet());
                shuffle((List<String>) racks);
            }

            // grab a random member of up to two racks
            List<InetAddress> result = new ArrayList<>(2);
            for (String rack : Iterables.limit(racks, 2))
            {
                List<InetAddress> rackMembers = validated.get(rack);
                result.add(rackMembers.get(getRandomInt(rackMembers.size())));
            }

            return result;
        }

        @VisibleForTesting
        protected boolean isValid(InetAddress input)
        {
            return !input.equals(FBUtilities.getBroadcastAddress()) && FailureDetector.instance.isAlive(input);
        }

        @VisibleForTesting
        protected int getRandomInt(int bound)
        {
            return ThreadLocalRandom.current().nextInt(bound);
        }

        @VisibleForTesting
        protected void shuffle(List<?> list)
        {
            Collections.shuffle(list);
        }
    }
}
