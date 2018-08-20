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

package org.apache.cassandra.auth;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.util.concurrent.MoreExecutors;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthCache<K, V> implements AuthCacheMBean<K>
{
    private static final Logger logger = LoggerFactory.getLogger(AuthCache.class);

    private static final String MBEAN_NAME_BASE = "org.apache.cassandra.auth:type=";

    /**
     * Underlying cache. LoadingCache will call underlying load function on {@link #get} if key is not present
     */
    protected volatile LoadingCache<K, V> cache;

    private String name;
    private IntConsumer setValidityDelegate;
    private IntSupplier getValidityDelegate;
    private IntConsumer setUpdateIntervalDelegate;
    private IntSupplier getUpdateIntervalDelegate;
    private IntConsumer setMaxEntriesDelegate;
    private IntSupplier getMaxEntriesDelegate;
    private Function<K, V> loadFunction;
    private CacheLoader<K, V> loader;
    private BooleanSupplier enableCache;

    /**
     * Prefer {@link Builder} over this ctor. It's purely here for cases that need to extend {@link AuthCache}
     * @param name
     * @param setValidityDelegate
     * @param getValidityDelegate
     * @param setUpdateIntervalDelegate
     * @param getUpdateIntervalDelegate
     * @param setMaxEntriesDelegate
     * @param getMaxEntriesDelegate
     * @param loadFunction
     * @param loader
     * @param isCacheEnabled
     */
    protected AuthCache(String name,
                        IntConsumer setValidityDelegate,
                        IntSupplier getValidityDelegate,
                        IntConsumer setUpdateIntervalDelegate,
                        IntSupplier getUpdateIntervalDelegate,
                        IntConsumer setMaxEntriesDelegate,
                        IntSupplier getMaxEntriesDelegate,
                        Function<K, V> loadFunction,
                        CacheLoader<K, V> loader,
                        BooleanSupplier isCacheEnabled)
    {
        this.name = name;
        this.setValidityDelegate = setValidityDelegate;
        this.getValidityDelegate = getValidityDelegate;
        this.setUpdateIntervalDelegate = setUpdateIntervalDelegate;
        this.getUpdateIntervalDelegate = getUpdateIntervalDelegate;
        this.setMaxEntriesDelegate = setMaxEntriesDelegate;
        this.getMaxEntriesDelegate = getMaxEntriesDelegate;
        this.loadFunction = loadFunction;
        this.loader = loader;
        this.enableCache = isCacheEnabled;

        init();
    }


    private AuthCache() { /* Used by builder only. */ }

    /**
     * Do all necessary validation and setup for the cache plus MBean.
     */
    protected void init()
    {
        validate();
        cache = initCache(null);
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, getObjectName());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    protected ObjectName getObjectName() throws MalformedObjectNameException
    {
        return new ObjectName(MBEAN_NAME_BASE + name);
    }

    /**
     * Validate the cache config and set reasonable defaults.
     * Will set the following defaults:
     * - Cache is enabled.
     * - Key expiry policy to {@link DatabaseDescriptor#getCredentialsValidity()}
     * - Key refresh policy to {@link DatabaseDescriptor#getCredentialsUpdateInterval()}
     * - Max cache entries to {@link DatabaseDescriptor#getCredentialsCacheMaxEntries()}
     */
    protected void validate()
    {
        // By default we will enable cache
        if (enableCache == null)
            enableCache = () -> true;

        if (!enableCache.getAsBoolean()){
            // At the expense of an even more complicated builder pattern we'll just throw an RTE
            // if someone created a cache with the cache disabled but didn't specify a loadFunction.
            if (loadFunction == null)
                throw new RuntimeException("You must at least specify a load function if cache is disabled.");
            // We still use a load function if we haven't enabled the cache, but there's no point having a CacheLoader.
            loader = null;
        }

        // Sensible defaults. These are the main use case and no others exist (yet)
        if (setValidityDelegate == null)
            setValidityDelegate = DatabaseDescriptor::setCredentialsValidity;
        if (getValidityDelegate == null)
            getValidityDelegate = DatabaseDescriptor::getCredentialsValidity;
        if (setUpdateIntervalDelegate == null)
            setUpdateIntervalDelegate = DatabaseDescriptor::setCredentialsUpdateInterval;
        if (getUpdateIntervalDelegate == null)
            getUpdateIntervalDelegate = DatabaseDescriptor::getCredentialsUpdateInterval;
        if (setMaxEntriesDelegate == null)
            setMaxEntriesDelegate = DatabaseDescriptor::setCredentialsCacheMaxEntries;
        if (getMaxEntriesDelegate == null)
            getMaxEntriesDelegate = DatabaseDescriptor::getCredentialsCacheMaxEntries;
    }

    /**
     * Retrieve a value from the cache. Will call {@link LoadingCache#get(Object)} which will
     * "load" the value if it's not present, thus populating the key.
     * @param k
     * @return The current value of {@code K} if cached or loaded.
     *
     * See {@link LoadingCache#get(Object)} for possible exceptions.
     */
    public V get(K k)
    {
        if (cache == null)
            return loadFunction.apply(k);

        return cache.get(k);
    }

    /**
     * Invalidate the entire cache.
     */
    public void invalidate()
    {
        cache = initCache(null);
    }

    /**
     * Invalidate a key
     * @param k key to invalidate
     */
    public void invalidate(K k)
    {
        if (cache != null)
            cache.invalidate(k);
    }

    /**
     * Time in milliseconds that a value in the cache will expire after.
     * @param validityPeriod in milliseconds
     */
    public void setValidity(int validityPeriod)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setValidityDelegate.accept(validityPeriod);
        cache = initCache(cache);
    }

    public int getValidity()
    {
        return getValidityDelegate.getAsInt();
    }

    /**
     * Time in milliseconds after which an entry in the cache should be refreshed (it's load function called again)
     * @param updateInterval in milliseconds
     */
    public void setUpdateInterval(int updateInterval)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setUpdateIntervalDelegate.accept(updateInterval);
        cache = initCache(cache);
    }

    public int getUpdateInterval()
    {
        return getUpdateIntervalDelegate.getAsInt();
    }

    /**
     * Set maximum number of entries in the cache.
     * @param maxEntries
     */
    public void setMaxEntries(int maxEntries)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setMaxEntriesDelegate.accept(maxEntries);
        cache = initCache(cache);
    }

    public int getMaxEntries()
    {
        return getMaxEntriesDelegate.getAsInt();
    }

    /**
     * (Re-)initialise the underlying cache. Will update validity, max entries, and update interval if
     * any have changed. Uses configured {@code loader} for the {@link CacheLoader} if available, otherwise will use
     * {@code loadFunction}.
     * Note: If you need some unhandled cache setting to be set you should extend {@link AuthCache} and override this method.
     * @param existing If not null will only update cache update validity, max entries, and update interval.
     * @return New {@link LoadingCache} if existing was null, otherwise the existing {@code cache}
     */
    protected LoadingCache<K, V> initCache(LoadingCache<K, V> existing)
    {
        if (!enableCache.getAsBoolean())
            return null;

        if (getValidity() <= 0)
            return null;

        logger.info("(Re)initializing {} (validity period/update interval/max entries) ({}/{}/{})",
                    name, getValidity(), getUpdateInterval(), getMaxEntries());

        if (existing == null) {
            // Use CacheLoader if it's been provided as it allows specifying methods that throw exceptions
            if (loader != null)
                return Caffeine.newBuilder()
                            .refreshAfterWrite(getUpdateInterval(), TimeUnit.MILLISECONDS)
                            .expireAfterWrite(getValidity(), TimeUnit.MILLISECONDS)
                            .maximumSize(getMaxEntries())
                            .executor(MoreExecutors.directExecutor())
                            .build(loader);
            // Otherwise just use provided function
            else
                return Caffeine.newBuilder()
                            .refreshAfterWrite(getUpdateInterval(), TimeUnit.MILLISECONDS)
                            .expireAfterWrite(getValidity(), TimeUnit.MILLISECONDS)
                            .maximumSize(getMaxEntries())
                            .executor(MoreExecutors.directExecutor())
                            .build(loadFunction::apply);
        }

        // Always set as mandatory
        cache.policy().refreshAfterWrite().ifPresent(policy ->
            policy.setExpiresAfter(getUpdateInterval(), TimeUnit.MILLISECONDS));
        cache.policy().expireAfterWrite().ifPresent(policy ->
            policy.setExpiresAfter(getValidity(), TimeUnit.MILLISECONDS));
        cache.policy().eviction().ifPresent(policy ->
            policy.setMaximum(getMaxEntries()));
        return cache;
    }

    public static class Builder<K, V>
    {
        private AuthCache<K, V> authCache = new AuthCache<>();
        /* We use two builders here to enforce users to specify at least one of loadFunction or loader rather than relying
         * on runtime checking.
         * Note that it's still possible to do a bad thing if you specify a CacheLoader w/o a LoadFunction but then for
         * some reason you disable the cache
         */

        /**
         * Simple case, pass your cache loading function in and a {@link LoadingCache} will be built from it.
         * Cannot throw exceptions and returned data will be loaded into the cache for the provided parameter (key).
         * @param loadFunc
         * @return
         */
        public Builder2 withLoadFunction(Function<K, V> loadFunc)
        {
            authCache.loadFunction = loadFunc;
            return new Builder2();
        }

        /**
         * Use a CacheLoader instead a function.
         * A CacheLoader will let you use a function that throws exceptions.
         * @param cacheLoader
         * @return
         */
        public Builder2 withCacheLoader(CacheLoader<K, V> cacheLoader)
        {
            authCache.loader = cacheLoader;
            return new Builder2();
        }

        public class Builder2
        {
            public Builder2 withLoadFunction(Function<K, V> loadFunc)
            {
                authCache.loadFunction = loadFunc;
                return this;
            }

            public Builder2 withCacheLoader(CacheLoader<K, V> cacheLoader)
            {
                authCache.loader = cacheLoader;
                return this;
            }

            public Builder2 withValidityConsumer(IntConsumer consumer)
            {
                authCache.setValidityDelegate = consumer;
                return this;
            }

            public Builder2 withValiditySupplier(IntSupplier supplier)
            {
                authCache.getValidityDelegate = supplier;
                return this;
            }

            public Builder2 withUpdateIntervalConsumer(IntConsumer consumer)
            {
                authCache.setUpdateIntervalDelegate = consumer;
                return this;
            }

            public Builder2 withUpdateIntervalSupplier(IntSupplier supplier)
            {
                authCache.getUpdateIntervalDelegate = supplier;
                return this;
            }

            public Builder2 withMaxEntriesConsumer(IntConsumer consumer)
            {
                authCache.setMaxEntriesDelegate = consumer;
                return this;
            }

            public Builder2 withMaxEntriesSupplier(IntSupplier supplier)
            {
                authCache.getMaxEntriesDelegate = supplier;
                return this;
            }

            public Builder2 withCacheEnabled(BooleanSupplier booleanSupplier)
            {
                authCache.enableCache = booleanSupplier;
                return this;
            }

            public AuthCache<K, V> build(String name)
            {
                authCache.name = name;
                // Validate config
                authCache.init();
                // Set up actual underlying cache.
                return authCache;
            }
        }
    }
}
