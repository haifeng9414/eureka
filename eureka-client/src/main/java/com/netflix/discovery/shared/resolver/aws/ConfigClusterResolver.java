package com.netflix.discovery.shared.resolver.aws;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.endpoint.EndpointUtils;
import com.netflix.discovery.shared.resolver.ClusterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A resolver that on-demand resolves from configuration what the endpoints should be.
 *
 * @author David Liu
 */
public class ConfigClusterResolver implements ClusterResolver<AwsEndpoint> {
    private static final Logger logger = LoggerFactory.getLogger(ConfigClusterResolver.class);

    private final EurekaClientConfig clientConfig;
    private final InstanceInfo myInstanceInfo;

    public ConfigClusterResolver(EurekaClientConfig clientConfig, InstanceInfo myInstanceInfo) {
        this.clientConfig = clientConfig;
        this.myInstanceInfo = myInstanceInfo;
    }

    @Override
    public String getRegion() {
        // 返回eureka.region属性的值，默认返回us-east-1
        return clientConfig.getRegion();
    }

    @Override
    public List<AwsEndpoint> getClusterEndpoints() {
        // 根据eureka.shouldUseDns属性的值决定是否使用dns解析eureka server的url，默认返回false
        if (clientConfig.shouldUseDnsForFetchingServiceUrls()) {
            if (logger.isInfoEnabled()) {
                logger.info("Resolving eureka endpoints via DNS: {}", getDNSName());
            }
            // 返回dns解析的eureka server地址
            return getClusterEndpointsFromDns();
        } else {
            logger.info("Resolving eureka endpoints via configuration");
            // 根据配置文件返回eureka server地址，当前实例所在的zone的eureka server地址在最前面
            return getClusterEndpointsFromConfig();
        }
    }

    private List<AwsEndpoint> getClusterEndpointsFromDns() {
        // 获取当前region的dns url，默认格式为txt.{region}.{eureka.eurekaServer.domainName}或者txt.{region}.{eureka.domainName}
        String discoveryDnsName = getDNSName();
        // 获取eureka.eurekaServer.port属性的值，默认返回eureka.port属性的值
        int port = Integer.parseInt(clientConfig.getEurekaServerPort());

        // cheap enough so just re-use
        // DnsTxtRecordClusterResolver能够解析dns并返回对应的ip地址
        DnsTxtRecordClusterResolver dnsResolver = new DnsTxtRecordClusterResolver(
                getRegion(),
                discoveryDnsName,
                true,
                port,
                false,
                clientConfig.getEurekaServerURLContext() // eureka.eurekaServer.context属性的值
        );

        // 获取dns的解析结果
        List<AwsEndpoint> endpoints = dnsResolver.getClusterEndpoints();

        if (endpoints.isEmpty()) {
            logger.error("Cannot resolve to any endpoints for the given dnsName: {}", discoveryDnsName);
        }

        return endpoints;
    }

    private List<AwsEndpoint> getClusterEndpointsFromConfig() {
        // 获取eureka.{region}.{availabilityZones}属性，默认返回defaultZone，按照逗号分隔
        String[] availZones = clientConfig.getAvailabilityZones(clientConfig.getRegion());
        // 获取当前实例的zone，默认返回availZones的第0个
        String myZone = InstanceInfo.getZone(availZones, myInstanceInfo);

        // clientConfig.shouldPreferSameZoneEureka()方法返回eureka.preferSameZone属性，表示是否优先使用同一个zone的实例
        // getServiceUrlsMapFromConfig方法根据配置文件的配置返回zone下eureka server的列表，返回的Map是LinkedHashMap，当前实例所在
        // 的zone排在最前面
        Map<String, List<String>> serviceUrls = EndpointUtils
                .getServiceUrlsMapFromConfig(clientConfig, myZone, clientConfig.shouldPreferSameZoneEureka());

        List<AwsEndpoint> endpoints = new ArrayList<>();
        for (String zone : serviceUrls.keySet()) {
            for (String url : serviceUrls.get(zone)) {
                try {
                    endpoints.add(new AwsEndpoint(url, getRegion(), zone));
                } catch (Exception ignore) {
                    logger.warn("Invalid eureka server URI: {}; removing from the server pool", url);
                }
            }
        }

        logger.debug("Config resolved to {}", endpoints);

        if (endpoints.isEmpty()) {
            logger.error("Cannot resolve to any endpoints from provided configuration: {}", serviceUrls);
        }

        return endpoints;
    }

    private String getDNSName() {
        return "txt." + getRegion() + '.' + clientConfig.getEurekaServerDNSName();
    }
}
