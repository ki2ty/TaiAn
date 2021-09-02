package com.taian.test.snapshot;

import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.datasource.SequoiadbDatasource;
import com.taian.test.datasource.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName SnapshotTest
 * @Author zlc
 * @Date 2021/9/1 下午5:31
 * @Description ShapshotTest
 * @Version 1.0
 */
@Slf4j
public class SnapshotTest {

    static final String HOST = System.getProperty("host", "152.136.105.119");
    static final int PORT = Integer.parseInt(System.getProperty("port", "11810"));
    static final String USERNAME = System.getProperty("username", "");
    static final String PASSWORD = System.getProperty("password", "");
    static Map<String, String> HOST_IP_MAP = new HashMap<>();

    static final SequoiadbDatasource dataSource = DataSource.getInstance().getSequoiadbDatasource();

    public static void main(String[] args) {

        HOST_IP_MAP.put("VM-0-16-centos", "152.136.105.119");
        HOST_IP_MAP.put("VM-0-12-centos", "49.232.166.21");

        BSONObject nodeTree = new BasicBSONObject();
        Map<String, Boolean> groupMap = new HashMap<>();
        try (Sequoiadb sdb = new Sequoiadb(HOST + ":" + PORT, USERNAME, PASSWORD)) {
            //获取SDB_SNAP_DATABASE快照游标
            DBCursor snap_cursor = sdb.getSnapshot(Sequoiadb.SDB_SNAP_DATABASE, "", "", "");
            while (snap_cursor.hasNext()) {
                BSONObject snapshot = snap_cursor.getNext();
                //拿到当前数据库连接请求数量
                String totalNumConnects = String.valueOf(snapshot.get("TotalNumConnects"));
                log.info("totalNumConnects: [{}]", totalNumConnects);
            }

            //获取集群节点列表
            DBCursor list_cursor = sdb.getList(Sequoiadb.SDB_LIST_GROUPS, null, null, null);
            while (list_cursor.hasNext()) {
                BSONObject list_cursorNext = list_cursor.getNext();
                BasicBSONList node_array = (BasicBSONList) list_cursorNext.get("Group");
                //获取组的HasPrimary指标
                groupMap.put(String.valueOf(list_cursorNext.get("GroupName")), list_cursorNext.containsField("PrimaryNode"));
                for (Object obj : node_array) {
                    BSONObject node = (BSONObject) obj;
                    String service_name = (String) ((BSONObject) ((BasicBSONList) node.get("Service")).get(0)).get("Name");
                    BasicBSONList bsonList;
                    if (!nodeTree.containsField((String) node.get("HostName"))) {
                        //节点树不存在当前节点的主机名
                        bsonList = new BasicBSONList();
                        log.info("hostname: [{}]", node.get("HostName"));
                    } else {
                        bsonList = (BasicBSONList) nodeTree.get((String) node.get("HostName"));
                    }
                    BasicBSONObject single_node = new BasicBSONObject("ServiceName", service_name);
                    // 0 -> data    1 -> coord  2 -> cata
                    single_node.put("role", list_cursorNext.get("Role"));
                    bsonList.add(single_node);
                    nodeTree.put((String) node.get("HostName"), bsonList);
                }
            }
            log.info("nodeTree: [{}]", nodeTree.toString());


        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("HasPrimary: [{}]", new BasicBSONObject(groupMap).toString());
        dataSource.removeCoord("152.136.105.119:11810");
        dataSource.removeCoord("49.232.166.21:11810");
        Set<String> keySet = nodeTree.keySet();
        keySet.forEach(k -> {
            BasicBSONList list = (BasicBSONList) nodeTree.get(k);
            for (Object o : list) {
                BSONObject obj = (BSONObject) o;
                getNodeMetrics(dataSource, HOST_IP_MAP.get(k), String.valueOf(obj.get("ServiceName")), Integer.parseInt(String.valueOf(obj.get("role"))) == 1);
            }
        });


    }


    private static void getNodeMetrics(SequoiadbDatasource ds, String host, String serviceName, boolean coord) {
        ds.addCoord(host + ":" + serviceName);
        try (Sequoiadb sdb = ds.getConnection()) {
            DBCursor snapshot = sdb.getSnapshot(6, "", "", "");
            while (snapshot.hasNext()) {
                BSONObject next = snapshot.getNext();
                if (coord) {
                    //协调节点的snapshot(6)只有TotalNumConnects
                    String totalNumConnects = String.valueOf(next.get("TotalNumConnects"));
                    log.info("host: [{}], serviceName: [{}], TotalNumConnects: [{}]", host, serviceName, totalNumConnects);
                } else {
                    //  Status IsPrimary ServiceStatus
                    String status = String.valueOf(next.get("Status"));
                    boolean isPrimary = Boolean.parseBoolean(String.valueOf(next.get("IsPrimary")));
                    boolean serviceStatus = Boolean.parseBoolean(String.valueOf(next.get("ServiceStatus")));
                    log.info("host: [{}], serviceName: [{}], status: [{}], isPrimary: [{}], serviceStatus: [{}]", host, serviceName, status, isPrimary, serviceStatus);
                }
            }
            sdb.releaseResource();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            ds.removeCoord(host + ":" + serviceName);
        }


    }


}
