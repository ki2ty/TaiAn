package com.taian.test.snapshot;

import com.sequoiacm.client.common.ScmType;
import com.sequoiacm.client.core.*;
import com.sequoiacm.client.element.ScmFileBasicInfo;
import com.sequoiacm.client.element.ScmFileStatisticInfo;
import com.sequoiacm.client.element.ScmWorkspaceInfo;
import com.sequoiacm.client.exception.ScmException;
import com.sequoiacm.infrastructure.statistics.common.ScmTimeAccuracy;
import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName SCMDemo
 * @Author zlc
 * @Date 2021/9/8 上午11:27
 * @Description SCMDemo
 * @Version 1.0
 */
@Slf4j
public class SCMDemo {

    static final String WORKSPACE = System.getProperty("workspace","test_test");
    static final String USER = System.getProperty("user","test_user");

    //文件上传下载相关指标
    public static void main(String[] args) throws ScmException, ParseException {

        ScmSession session = ScmFactory.Session.createSession(new ScmConfigOption("152.136.105.119:8990/rootSite", "admin", "admin"));

        ScmFileStatistician s = ScmSystem.Statistics.fileStatistician(session);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date begin = simpleDateFormat.parse("2021-09-15 06:00:00");
        Date end = simpleDateFormat.parse("2021-09-15 12:00:00");

        // 查询 user1 在 2021-01-01 至 2021-01-03 （不包含 2021-01-03）时间段内工作区 workspace1 下的文件上传统计信息
        ScmFileStatisticInfo uploadData = s.upload().beginDate(begin).endDate(end).workspace("test_test").user("admin")
                .timeAccuracy(ScmTimeAccuracy.HOUR).get();

        // 查询 user1 在 2021-01-01 至 2021-01-03 （不包含 2021-01-03）时间段内工作区 workspace1 下的文件下载统计信息
        ScmFileStatisticInfo downloadData = s.download().beginDate(begin).endDate(end).workspace("test_test").user("admin")
                .timeAccuracy(ScmTimeAccuracy.HOUR).get();

        log.info("文件上传 -> 请求总数: [{}], 平均上传文件大小: [{}], 平均响应时间: [{}]",
                uploadData.getRequestCount(),
                uploadData.getAvgTrafficSize(),
                uploadData.getAvgResponseTime());

        log.info("文件下载 -> 请求总数: [{}], 平均下载文件大小: [{}], 平均响应时间: [{}]",
                downloadData.getRequestCount(),
                downloadData.getAvgTrafficSize(),
                downloadData.getAvgResponseTime());

        listHealth();

        test(session);

        test_dir(session);

    }


    //获取全部节点服务的状态
    private static void listHealth() throws ScmException {

        ScmSession session = ScmFactory.Session.createSession(new ScmConfigOption("152.136.105.119:8990/rootSite", "admin", "admin"));

        ScmCursor<ScmHealth> healthCursor=ScmSystem.Monitor.listHealth(session, null);
        while(healthCursor.hasNext()) {
            ScmHealth health=healthCursor.getNext();
            log.info("listHealth: nodeName="+ health.getNodeName()
                    + ", serviceName=" +health.getServiceName()
                    + ", status="+ health.getStatus());
        }
        healthCursor.close();
    }


    private static void test(ScmSession session) throws ScmException {
        // 手动刷新 test_ws 工作区文件访问量
        ScmSystem.Statistics.refresh(session, ScmType.StatisticsType.TRAFFIC, "test_test");
        // 文件访问量统计
        ScmCursor<ScmStatisticsTraffic> trafficCursor = ScmSystem.Statistics.listTraffic(session,
                ScmQueryBuilder.start().put(ScmAttributeName.FileDelta.WORKSPACE_NAME).is("test_test")
                        .get());
        while (trafficCursor.hasNext()) {
            ScmStatisticsTraffic traffic = trafficCursor.getNext();
            log.info("listTraffic: workspaceName=" + traffic.getWorkspaceName()
                    + ", trafficCount=" + traffic.getTraffic()
                    + ", trafficType=" + traffic.getType()
                    + ", recordTime=" + traffic.getRecordTime());
        }
        trafficCursor.close();
        // 手动刷新 test_ws 工作区文件增量
        ScmSystem.Statistics.refresh(session, ScmType.StatisticsType.FILE_DELTA, "test_test");
        // 文件增量统计
        ScmCursor<ScmStatisticsFileDelta> fileDeltaCursor = ScmSystem.Statistics
                .listFileDelta(session, ScmQueryBuilder.start()
                        .put(ScmAttributeName.FileDelta.WORKSPACE_NAME).is("test_test").get());
        while (fileDeltaCursor.hasNext()) {
            ScmStatisticsFileDelta fileDelta = fileDeltaCursor.getNext();
            log.info("listFileDelta: workspaceName=" + fileDelta.getWorkspaceName()
                    + ", deltaCount=" + fileDelta.getCountDelta()
                    + ", deltaSize=" + fileDelta.getSizeDelta()
                    + ", recordTime=" + fileDelta.getRecordTime());
        }
        fileDeltaCursor.close();
    }


    //维度：ws，指标：目录总数，文件综述，数据总量大小
    private static void test_dir(ScmSession session) throws ScmException {

        session = ScmFactory.Session.createSession(new ScmConfigOption("152.136.105.119:8990/rootSite", "test_user", "test_user"));

        ScmCursor<ScmWorkspaceInfo> workspaces = ScmFactory.Workspace.listWorkspace(session);

        // 根据路径创建目录： /a
        //        ScmWorkspace test_ws = ScmFactory.Workspace.getWorkspace("test_test", session);
        //        ScmDirectory test_dir = ScmFactory.Directory.createInstance(test_ws, "/a");
        while(workspaces.hasNext()){
            ScmWorkspaceInfo nextWorkSpace = workspaces.getNext();
            ScmWorkspace workspace = ScmFactory.Workspace.getWorkspace(nextWorkSpace.getName(), session);
            log.info("enable directory: [{}]", workspace.isEnableDirectory());
            if(workspace.isEnableDirectory()){
                AtomicInteger file_count = new AtomicInteger();
                AtomicInteger dir_count = new AtomicInteger();
                ScmDirectory directory = ScmFactory.Directory.getInstance(workspace, "/");
                getDirCount(directory, dir_count, file_count);
                log.info("dir_count: [{}], file_count: [{}]", dir_count.get(), file_count.get());
            }
        }
    }

    private static void getDirCount(ScmDirectory directory, AtomicInteger dir_count, AtomicInteger file_count) throws ScmException {
        ScmCursor<ScmDirectory> scmDirectoryScmCursor = directory.listDirectories(null);
        ScmCursor<ScmFileBasicInfo> scmFileBasicInfoScmCursor = directory.listFiles(null);
        while(scmDirectoryScmCursor.hasNext()){
            ScmDirectory next = scmDirectoryScmCursor.getNext();
            log.debug("dir_name: [{}]", next.getName());
            dir_count.getAndIncrement();
            getDirCount(next, dir_count, file_count);
        }
        while (scmFileBasicInfoScmCursor.hasNext()) {
            ScmFileBasicInfo next = scmFileBasicInfoScmCursor.getNext();
            log.debug("file_name: [{}]", next.getFileName());
            file_count.getAndIncrement();
        }
    }

}
