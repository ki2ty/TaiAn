package com.taian.test.snapshot;

import com.sequoiacm.client.core.*;
import com.sequoiacm.client.element.ScmFileStatisticInfo;
import com.sequoiacm.client.exception.ScmException;
import com.sequoiacm.infrastructure.statistics.common.ScmTimeAccuracy;
import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName SCMDemo
 * @Author zlc
 * @Date 2021/9/8 上午11:27
 * @Description SCMDemo
 * @Version 1.0
 */
@Slf4j
public class SCMDemo {

    static final String WORKSPACE = System.getProperty("workspace","workspace1");
    static final String USER = System.getProperty("user","user1");

    //文件上传下载相关指标
    public static void main(String[] args) throws ScmException, ParseException {

        ScmSession session = ScmFactory.Session.createSession(new ScmConfigOption("scmserver:8080/rootsite", "test_user", "scmPassword"));

        ScmFileStatistician s = ScmSystem.Statistics.fileStatistician(session);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date begin = simpleDateFormat.parse("2021-01-01");
        Date end = simpleDateFormat.parse("2021-09-01");

        // 查询 user1 在 2021-01-01 至 2021-01-03 （不包含 2021-01-03）时间段内工作区 workspace1 下的文件上传统计信息
        ScmFileStatisticInfo uploadData = s.upload().beginDate(begin).endDate(end).user(USER)
                .timeAccuracy(ScmTimeAccuracy.DAY).get();

        // 查询 user1 在 2021-01-01 至 2021-01-03 （不包含 2021-01-03）时间段内工作区 workspace1 下的文件下载统计信息
        ScmFileStatisticInfo downloadData = s.download().beginDate(begin).endDate(end).user(USER)
                .timeAccuracy(ScmTimeAccuracy.DAY).get();

        log.info("文件上传 -> 请求总数: [{}], 平均上传文件大小: [{}], 平均响应时间: [{}]",
                uploadData.getRequestCount(),
                uploadData.getAvgTrafficSize(),
                uploadData.getAvgResponseTime());

        log.info("文件下载 -> 请求总数: [{}], 平均下载文件大小: [{}], 平均响应时间: [{}]",
                downloadData.getRequestCount(),
                downloadData.getAvgTrafficSize(),
                downloadData.getAvgResponseTime());

    }


    //获取全部节点服务的状态
    private static void listHealth() throws ScmException {

        ScmSession session = ScmFactory.Session.createSession(new ScmConfigOption("scmserver:8080/rootsite", "test_user", "scmPassword"));

        ScmCursor<ScmHealth> healthCursor=ScmSystem.Monitor.listHealth(session, null);
        while(healthCursor.hasNext()) {
            ScmHealth health=healthCursor.getNext();
            System.out.println("listHealth: nodeName="+ health.getNodeName()
                    + ", serviceName=" +health.getServiceName()
                    + ", status="+ health.getStatus());
        }
        healthCursor.close();
    }

}
