package com.taian.test.datasource;

import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.datasource.ConnectStrategy;
import com.sequoiadb.datasource.DatasourceOptions;
import com.sequoiadb.datasource.SequoiadbDatasource;

import java.util.ArrayList;

/**
 * @ClassName DataSource
 * @Author zlc
 * @Date 2021/9/1 下午7:45
 * @Description DataSource
 * @Version 1.0
 */
public class DataSource {

    private final SequoiadbDatasource sequoiadbDatasource;

    private static class DataSourceHolder {
        private static final DataSource INSTANCE = new DataSource();
    }

    private DataSource() {
        ArrayList<String> addrs = new ArrayList<String>();
        String user = "";
        String password = "";
        ConfigOptions nwOpt = new ConfigOptions();
        DatasourceOptions dsOpt = new DatasourceOptions();
        SequoiadbDatasource ds = null;
        // 提供coord节点地址
        addrs.add("152.136.105.119:11810");
        addrs.add("49.232.166.21:11810");

        // 设置网络参数
        nwOpt.setConnectTimeout(500);                      // 建连超时时间为 500ms
        nwOpt.setMaxAutoConnectRetryTime(0);               // 建连失败后重试时间为 0ms

        // 设置连接池参数
        dsOpt.setMaxCount(500);                            // 连接池最多能提供 500 个连接
        dsOpt.setDeltaIncCount(20);                        // 每次增加 20 个连接
        dsOpt.setMaxIdleCount(20);                         // 连接池空闲时，保留 20 个连接

        dsOpt.setKeepAliveTimeout(0);                      // 池中空闲连接存活时间，单位:毫秒
        // 0 表示不关心连接隔多长时间没有收发消息

        dsOpt.setCheckInterval(60 * 1000);                 // 每隔 60s 将连接池中多于 MaxIdleCount 限定的
        // 空闲连接关闭，并将存活时间过长（连接已停止收
        // 发超过 keepAliveTimeout 时间）的连接关闭

        dsOpt.setSyncCoordInterval(0);                     // 向 catalog 同步 coord 地址的周期，单位:毫秒
        // 0 表示不同步

        dsOpt.setValidateConnection(false);                // 连接出池时，是否检测连接的可用性，默认不检测
        dsOpt.setConnectStrategy(ConnectStrategy.BALANCE); // 默认使用 coord 地址负载均衡的策略获取连接

        // 建立连接池
        sequoiadbDatasource = new SequoiadbDatasource(addrs, user, password, nwOpt, dsOpt);
    }

    public static final DataSource getInstance() {
        return DataSourceHolder.INSTANCE;
    }

    public SequoiadbDatasource getSequoiadbDatasource() {
        return sequoiadbDatasource;
    }
}
