/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.shared.transport;

import java.util.List;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.EurekaClientNames;
import com.netflix.discovery.shared.resolver.AsyncResolver;
import com.netflix.discovery.shared.resolver.ClosableResolver;
import com.netflix.discovery.shared.resolver.ClusterResolver;
import com.netflix.discovery.shared.resolver.EndpointRandomizer;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.resolver.aws.ApplicationsResolver;
import com.netflix.discovery.shared.resolver.aws.AwsEndpoint;
import com.netflix.discovery.shared.resolver.aws.ConfigClusterResolver;
import com.netflix.discovery.shared.resolver.aws.EurekaHttpResolver;
import com.netflix.discovery.shared.resolver.aws.ZoneAffinityClusterResolver;
import com.netflix.discovery.shared.transport.decorator.SessionedEurekaHttpClient;
import com.netflix.discovery.shared.transport.decorator.RedirectingEurekaHttpClient;
import com.netflix.discovery.shared.transport.decorator.RetryableEurekaHttpClient;
import com.netflix.discovery.shared.transport.decorator.ServerStatusEvaluators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tomasz Bak
 */
public final class EurekaHttpClients {

    private static final Logger logger = LoggerFactory.getLogger(EurekaHttpClients.class);

    private EurekaHttpClients() {
    }

    public static EurekaHttpClientFactory queryClientFactory(ClusterResolver bootstrapResolver,
                                                             TransportClientFactory transportClientFactory,
                                                             EurekaClientConfig clientConfig,
                                                             EurekaTransportConfig transportConfig,
                                                             InstanceInfo myInstanceInfo,
                                                             ApplicationsResolver.ApplicationsSource applicationsSource,
                                                             EndpointRandomizer randomizer
    ) {

        // 当eureka.useBootstrapResolverForQuery属性为true时使用wrapClosable(bootstrapResolver)作为ClosableResolver接口的
        // 实现类，默认为true，wrapClosable方法只是返回一个适配器
        ClosableResolver queryResolver = transportConfig.useBootstrapResolverForQuery()
                ? wrapClosable(bootstrapResolver)
                : queryClientResolver(bootstrapResolver, transportClientFactory,
                clientConfig, transportConfig, myInstanceInfo, applicationsSource, randomizer);
        return canonicalClientFactory(EurekaClientNames.QUERY, transportConfig, queryResolver, transportClientFactory);
    }

    public static EurekaHttpClientFactory registrationClientFactory(ClusterResolver bootstrapResolver,
                                                                    TransportClientFactory transportClientFactory,
                                                                    EurekaTransportConfig transportConfig) {
        return canonicalClientFactory(EurekaClientNames.REGISTRATION, transportConfig, bootstrapResolver, transportClientFactory);
    }

    static EurekaHttpClientFactory canonicalClientFactory(final String name,
                                                          final EurekaTransportConfig transportConfig,
                                                          final ClusterResolver<EurekaEndpoint> clusterResolver,
                                                          final TransportClientFactory transportClientFactory) {

        return new EurekaHttpClientFactory() {
            @Override
            public EurekaHttpClient newClient() {
                return new SessionedEurekaHttpClient(
                        name,
                        RetryableEurekaHttpClient.createFactory( // 添加了retry功能
                                name,
                                transportConfig,
                                clusterResolver,
                                RedirectingEurekaHttpClient.createFactory(transportClientFactory), // 添加了处理302状态码并重定向的功能
                                ServerStatusEvaluators.legacyEvaluator()),
                        transportConfig.getSessionedClientReconnectIntervalSeconds() * 1000
                );
            }

            @Override
            public void shutdown() {
                wrapClosable(clusterResolver).shutdown();
            }
        };
    }

    // ==================================
    // Resolvers for the client factories
    // ==================================

    public static final String COMPOSITE_BOOTSTRAP_STRATEGY = "composite";

    public static ClosableResolver<AwsEndpoint> newBootstrapResolver(
            final EurekaClientConfig clientConfig,
            final EurekaTransportConfig transportConfig,
            final TransportClientFactory transportClientFactory,
            final InstanceInfo myInstanceInfo,
            final ApplicationsResolver.ApplicationsSource applicationsSource,
            final EndpointRandomizer randomizer) {
        // 判断eureka.bootstrapResolverStrategy属性的值是否等于composite，默认该属性为null
        if (COMPOSITE_BOOTSTRAP_STRATEGY.equals(transportConfig.getBootstrapResolverStrategy())) {
            if (clientConfig.shouldFetchRegistry()) {
                return compositeBootstrapResolver(
                        clientConfig,
                        transportConfig,
                        transportClientFactory,
                        myInstanceInfo,
                        applicationsSource,
                        randomizer
                );
            } else {
                logger.warn("Cannot create a composite bootstrap resolver if registry fetch is disabled." +
                        " Falling back to using a default bootstrap resolver.");
            }
        }

        // if all else fails, return the default
        // 创建一个AsyncResolver对象，能够从配置文件或者dns解析eureka server地址列表，并定时更新
        return defaultBootstrapResolver(clientConfig, myInstanceInfo, randomizer);
    }

    /**
     * @return a bootstrap resolver that resolves eureka server endpoints based on either DNS or static config,
     *         depending on configuration for one or the other. This resolver will warm up at the start.
     */
    static ClosableResolver<AwsEndpoint> defaultBootstrapResolver(final EurekaClientConfig clientConfig,
                                                                  final InstanceInfo myInstanceInfo,
                                                                  final EndpointRandomizer randomizer) {
        // 如果没有设置eureka.region属性，则默认获取eureka.us-east-1.availabilityZones属性的值，默认为defaultZone
        String[] availZones = clientConfig.getAvailabilityZones(clientConfig.getRegion());
        // 默认获取availZones的第0个
        String myZone = InstanceInfo.getZone(availZones, myInstanceInfo);

        // ZoneAffinityClusterResolver用于返回AwsEndpoint列表，并将zone等于传入的myZone的AwsEndpoint对象放在列表的前面
        ClusterResolver<AwsEndpoint> delegateResolver = new ZoneAffinityClusterResolver(
                new ConfigClusterResolver(clientConfig, myInstanceInfo), // ConfigClusterResolver为ZoneAffinityClusterResolver提供AwsEndpoint列表
                myZone,
                true,
                randomizer
        );

        // 获取eureka server地址列表
        List<AwsEndpoint> initialValue = delegateResolver.getClusterEndpoints();
        if (initialValue.isEmpty()) {
            String msg = "Initial resolution of Eureka server endpoints failed. Check ConfigClusterResolver logs for more info";
            logger.error(msg);
            failFastOnInitCheck(clientConfig, msg);
        }

        // AsyncResolver类会定时调用delegateResolver.getClusterEndpoints()方法，注意当AsyncResolver对象的getClusterEndpoints方法
        // 第一次被调用后定时任务才会开启
        // clientConfig.getEurekaServiceUrlPollIntervalSeconds()方法返回eureka.serviceUrlPollIntervalMs属性，该属性决定AsyncResolver
        // 定时调用的周期
        return new AsyncResolver<>(
                EurekaClientNames.BOOTSTRAP,
                delegateResolver,
                initialValue,
                1,
                clientConfig.getEurekaServiceUrlPollIntervalSeconds() * 1000
        );
    }

    /**
     * @return a bootstrap resolver that resolves eureka server endpoints via a remote call to a "vip source"
     *         the local registry, where the source is found from a rootResolver (dns or config)
     */
    static ClosableResolver<AwsEndpoint> compositeBootstrapResolver(
            final EurekaClientConfig clientConfig,
            final EurekaTransportConfig transportConfig,
            final TransportClientFactory transportClientFactory,
            final InstanceInfo myInstanceInfo,
            final ApplicationsResolver.ApplicationsSource applicationsSource,
            final EndpointRandomizer randomizer)
    {
        final ClusterResolver rootResolver = new ConfigClusterResolver(clientConfig, myInstanceInfo);

        final EurekaHttpResolver remoteResolver = new EurekaHttpResolver(
                clientConfig,
                transportConfig,
                rootResolver,
                transportClientFactory,
                transportConfig.getWriteClusterVip()
        );

        final ApplicationsResolver localResolver = new ApplicationsResolver(
                clientConfig,
                transportConfig,
                applicationsSource,
                transportConfig.getWriteClusterVip()
        );

        ClusterResolver<AwsEndpoint> compositeResolver = new ClusterResolver<AwsEndpoint>() {
            @Override
            public String getRegion() {
                return clientConfig.getRegion();
            }

            @Override
            public List<AwsEndpoint> getClusterEndpoints() {
                List<AwsEndpoint> result = localResolver.getClusterEndpoints();
                if (result.isEmpty()) {
                    result = remoteResolver.getClusterEndpoints();
                }

                return result;
            }
        };

        List<AwsEndpoint> initialValue = compositeResolver.getClusterEndpoints();
        if (initialValue.isEmpty()) {
            String msg = "Initial resolution of Eureka endpoints failed. Check ConfigClusterResolver logs for more info";
            logger.error(msg);
            failFastOnInitCheck(clientConfig, msg);
        }

        String[] availZones = clientConfig.getAvailabilityZones(clientConfig.getRegion());
        String myZone = InstanceInfo.getZone(availZones, myInstanceInfo);

        return new AsyncResolver<>(
                EurekaClientNames.BOOTSTRAP,
                new ZoneAffinityClusterResolver(compositeResolver, myZone, true, randomizer),
                initialValue,
                transportConfig.getAsyncExecutorThreadPoolSize(),
                transportConfig.getAsyncResolverRefreshIntervalMs()
        );
    }

    /**
     * @return a resolver that resolves eureka server endpoints for query operations
     */
    static ClosableResolver<AwsEndpoint> queryClientResolver(final ClusterResolver bootstrapResolver,
                                                             final TransportClientFactory transportClientFactory,
                                                             final EurekaClientConfig clientConfig,
                                                             final EurekaTransportConfig transportConfig,
                                                             final InstanceInfo myInstanceInfo,
                                                             final ApplicationsResolver.ApplicationsSource applicationsSource,
                                                             final EndpointRandomizer randomizer) {
        final EurekaHttpResolver remoteResolver = new EurekaHttpResolver(
                clientConfig,
                transportConfig,
                bootstrapResolver,
                transportClientFactory,
                transportConfig.getReadClusterVip()
        );

        final ApplicationsResolver localResolver = new ApplicationsResolver(
                clientConfig,
                transportConfig,
                applicationsSource,
                transportConfig.getReadClusterVip()
        );

        return compositeQueryResolver(
                remoteResolver,
                localResolver,
                clientConfig,
                transportConfig,
                myInstanceInfo,
                randomizer
        );
    }

    /**
     * @return a composite resolver that resolves eureka server endpoints for query operations, given two resolvers:
     *         a resolver that can resolve targets via a remote call to a remote source, and a resolver that
     *         can resolve targets via data in the local registry.
     */
    /* testing */ static ClosableResolver<AwsEndpoint> compositeQueryResolver(
            final ClusterResolver<AwsEndpoint> remoteResolver,
            final ClusterResolver<AwsEndpoint> localResolver,
            final EurekaClientConfig clientConfig,
            final EurekaTransportConfig transportConfig,
            final InstanceInfo myInstanceInfo,
            final EndpointRandomizer randomizer) {
        String[] availZones = clientConfig.getAvailabilityZones(clientConfig.getRegion());
        String myZone = InstanceInfo.getZone(availZones, myInstanceInfo);

        ClusterResolver<AwsEndpoint> compositeResolver = new ClusterResolver<AwsEndpoint>() {
            @Override
            public String getRegion() {
                return clientConfig.getRegion();
            }

            @Override
            public List<AwsEndpoint> getClusterEndpoints() {
                List<AwsEndpoint> result = localResolver.getClusterEndpoints();
                if (result.isEmpty()) {
                    result = remoteResolver.getClusterEndpoints();
                }

                return result;
            }
        };

        return new AsyncResolver<>(
                EurekaClientNames.QUERY,
                new ZoneAffinityClusterResolver(compositeResolver, myZone, true, randomizer),
                transportConfig.getAsyncExecutorThreadPoolSize(),
                transportConfig.getAsyncResolverRefreshIntervalMs(),
                transportConfig.getAsyncResolverWarmUpTimeoutMs()
        );
    }


    static <T extends EurekaEndpoint> ClosableResolver<T> wrapClosable(final ClusterResolver<T> clusterResolver) {
        if (clusterResolver instanceof ClosableResolver) {
            return (ClosableResolver) clusterResolver;
        }

        return new ClosableResolver<T>() {
            @Override
            public void shutdown() {
                // no-op
            }

            @Override
            public String getRegion() {
                return clusterResolver.getRegion();
            }

            @Override
            public List<T> getClusterEndpoints() {
                return clusterResolver.getClusterEndpoints();
            }
        };
    }

    // potential future feature, guarding with experimental flag for now
    private static void failFastOnInitCheck(EurekaClientConfig clientConfig, String msg) {
        if ("true".equals(clientConfig.getExperimental("clientTransportFailFastOnInit"))) {
            throw new RuntimeException(msg);
        }
    }
}
