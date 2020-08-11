package com.netflix.appinfo.providers;

import javax.inject.Singleton;
import javax.inject.Provider;
import java.util.Map;

import com.google.inject.Inject;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.appinfo.InstanceInfo.PortType;
import com.netflix.appinfo.LeaseInfo;
import com.netflix.appinfo.RefreshableInstanceConfig;
import com.netflix.appinfo.UniqueIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InstanceInfo provider that constructs the InstanceInfo this this instance using
 * EurekaInstanceConfig.
 *
 * This provider is @Singleton scope as it provides the InstanceInfo for both DiscoveryClient
 * and ApplicationInfoManager, and need to provide the same InstanceInfo to both.
 *
 * @author elandau
 *
 */
@Singleton
public class EurekaConfigBasedInstanceInfoProvider implements Provider<InstanceInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(EurekaConfigBasedInstanceInfoProvider.class);

    private final EurekaInstanceConfig config;

    private InstanceInfo instanceInfo;

    @Inject(optional = true)
    private VipAddressResolver vipAddressResolver = null;

    @Inject
    public EurekaConfigBasedInstanceInfoProvider(EurekaInstanceConfig config) {
        this.config = config;
    }

    @Override
    public synchronized InstanceInfo get() {
        if (instanceInfo == null) {
            // Build the lease information to be passed to the server based on config
            // LeaseInfo对象用于保存实例的续租信息和配置
            LeaseInfo.Builder leaseInfoBuilder = LeaseInfo.Builder.newBuilder()
                    .setRenewalIntervalInSecs(config.getLeaseRenewalIntervalInSeconds()) // 设置向eureka server发送心跳的间隔，默认30s
                    .setDurationInSecs(config.getLeaseExpirationDurationInSeconds()); // 设置多久没向eureka server发送心跳时将当前实例从eureka server中移除，默认90s

            // 创建地址解析器，默认实现是解析符合正则\$\{(.*?)\}格式的字符串，在配置的属性中寻找对应的变量并替换，如${eureka.env}.domain.com
            if (vipAddressResolver == null) {
                vipAddressResolver = new Archaius1VipAddressResolver();
            }

            // Builder the instance information to be registered with eureka server
            InstanceInfo.Builder builder = InstanceInfo.Builder.newBuilder(vipAddressResolver);

            // set the appropriate id for the InstanceInfo, falling back to datacenter Id if applicable, else hostname
            // 获取instanceId，即eureka.instanceId
            String instanceId = config.getInstanceId();
            if (instanceId == null || instanceId.isEmpty()) {
                // 如果instanceId为空则尝试以dataCenter id为instanceId
                DataCenterInfo dataCenterInfo = config.getDataCenterInfo();
                if (dataCenterInfo instanceof UniqueIdentifier) {
                    instanceId = ((UniqueIdentifier) dataCenterInfo).getId();
                } else {
                    // 默认DataCenterInfo的实现是DataCenterInfo类而不是UniqueIdentifier类，所以默认instanceId是下面的config.getHostName(false)
                    // 的值，而config.getHostName(false)默认实现为返回hostname
                    instanceId = config.getHostName(false);
                }
            }

            String defaultAddress;
            if (config instanceof RefreshableInstanceConfig) {
                // Refresh AWS data center info, and return up to date address
                // aws环境下的config才会实现RefreshableInstanceConfig接口
                defaultAddress = ((RefreshableInstanceConfig) config).resolveDefaultAddress(false);
            } else {
                // defaultAddress默认使用hostname
                defaultAddress = config.getHostName(false);
            }

            // fail safe
            // 如果获取defaultAddress失败则使用ip地址
            if (defaultAddress == null || defaultAddress.isEmpty()) {
                defaultAddress = config.getIpAddress();
            }

            builder.setNamespace(config.getNamespace()) // 默认为eureka.
                    .setInstanceId(instanceId) // 默认是hostname
                    .setAppName(config.getAppname()) // eureka.name属性的值，默认为unknown
                    .setAppGroupName(config.getAppGroupName()) // eureka.appGroup属性的值，默认为unknown
                    .setDataCenterInfo(config.getDataCenterInfo()) // 默认返回MyOwn
                    .setIPAddr(config.getIpAddress()) // 默认为本机ip
                    .setHostName(defaultAddress) // 默认为hostname
                    .setPort(config.getNonSecurePort()) // eureka.port属性的值，默认80
                    .enablePort(PortType.UNSECURE, config.isNonSecurePortEnabled()) // 默认为true
                    .setSecurePort(config.getSecurePort()) // 默认443
                    .enablePort(PortType.SECURE, config.getSecurePortEnabled()) // 默认false
                    .setVIPAddress(config.getVirtualHostName()) // eureka.vipAddress属性的值，默认为hostname:getNonSecurePort()
                    .setSecureVIPAddress(config.getSecureVirtualHostName()) // 如果securePortEnabled属性为true，则为eureka.secureVipAddress属性的值，默认为hostname:getSecurePort()
                    .setHomePageUrl(config.getHomePageUrlPath(), config.getHomePageUrl()) // 默认为http://hostname:8080/
                    .setStatusPageUrl(config.getStatusPageUrlPath(), config.getStatusPageUrl()) // 默认为http://hostname:8080/Status
                    .setASGName(config.getASGName()) // 默认为null
                    .setHealthCheckUrls(config.getHealthCheckUrlPath(), // 默认为http://hostname:8080/healthcheck
                            config.getHealthCheckUrl(), config.getSecureHealthCheckUrl());


            // Start off with the STARTING state to avoid traffic
            if (!config.isInstanceEnabledOnit()) { // 判断eureka.traffic.enabled属性的值，默认为false
                // 设置初始状态为starting，该状态的实例不会接收流量，给了实例初始化自己的时间
                InstanceStatus initialStatus = InstanceStatus.STARTING;
                LOG.info("Setting initial instance status as: {}", initialStatus);
                builder.setStatus(initialStatus);
            } else {
                LOG.info("Setting initial instance status as: {}. This may be too early for the instance to advertise "
                         + "itself as available. You would instead want to control this via a healthcheck handler.",
                         InstanceStatus.UP);
            }

            // Add any user-specific metadata information
            // metadataMap保存了用户自定义的属性
            for (Map.Entry<String, String> mapEntry : config.getMetadataMap().entrySet()) {
                String key = mapEntry.getKey();
                String value = mapEntry.getValue();
                // only add the metadata if the value is present
                if (value != null && !value.isEmpty()) {
                    builder.add(key, value);
                }
            }

            instanceInfo = builder.build();
            instanceInfo.setLeaseInfo(leaseInfoBuilder.build());
        }
        return instanceInfo;
    }

}
