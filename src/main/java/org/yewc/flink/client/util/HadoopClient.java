package org.yewc.flink.client.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public final class HadoopClient {

    public static String DEFAULT_USER = "hadoop";

    public static String DEFAULT_NAMENODE = "hdfs://localhost:8020";

//    private static Map<String, FileSystem> fsMap = new HashMap<>();

    private static FileSystem getFs(String nameNode, String userName) throws Exception {
//        if (!fsMap.containsKey(nameNode)) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(nameNode), conf, StringUtils.isBlank(userName) ? DEFAULT_USER : userName);
            return fs;
//            fsMap.put(nameNode, fs);
//        }
//        return fsMap.get(nameNode);
    }

    /**
     * 往hdfs上传文件
     */
    public static void addFileToHdfs(String nameNode, String userName, String localPath, String hdfsPath) throws Exception {
        Path src = new Path(localPath);
        Path dst = new Path(hdfsPath);

        FileSystem fs = getFs(nameNode, userName);
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

    /**
     * 从hdfs中复制文件到本地文件系统
     */
    public static void downloadFileToLocal(String nameNode, String userName, String hdfsPath, String localPath) throws Exception {
        Path src = new Path(hdfsPath);
        Path dst = new Path(localPath);

        FileSystem fs = getFs(nameNode, userName);
        fs.copyToLocalFile(src, dst);
        fs.close();
    }

    public static void writeToHDFS(String nameNode, String userName, String hdfsPath, String data) throws Exception {
        FileSystem fs = getFs(nameNode, userName);
        Path path = new Path(hdfsPath);

        FSDataOutputStream out = fs.create(path);   //创建文件
//        out.writeBytes(data);
        out.write(data.getBytes("UTF-8"));
        out.close();
        fs.close();
    }

    /**
     * 查看最新匹配的文件名
     */
    public static JSONArray findListFile(String nameNode, String userName, String path) throws Exception {
        FileSystem fs = getFs(nameNode, userName);
        FileStatus[] listStatus = fs.listStatus(new Path(path));

        JSONArray fileJa = new JSONArray();
        for (FileStatus fstatus : listStatus) {
            JSONObject jo = new JSONObject();
            jo.put("name", fstatus.getPath().getName());
            jo.put("path", fstatus.getPath().toUri().getPath());
            jo.put("time", fstatus.getModificationTime());
            fileJa.put(jo);
        }
        fs.close();
        return fileJa;
    }

    /**
     * 存在文件
     */
    public static boolean existFile(String nameNode, String userName, String file) throws Exception {
        FileSystem fs = getFs(nameNode, userName);
        boolean result = fs.exists(new Path(file));
        fs.close();
        return result;
    }

    /**
     * 查看最新匹配的文件名
     */
    public static String findLastFile(String nameNode, String userName, String file, String containStr) throws Exception {
        FileSystem fs = getFs(nameNode, userName);
        FileStatus[] listStatus = fs.listStatus(new Path(file));

        String lastName = null;
        long maxTime = 0;
        for (FileStatus fstatus : listStatus) {
            String name = fstatus.getPath().getName();
            if (name.contains(containStr) && fstatus.getModificationTime() > maxTime) {
                maxTime = fstatus.getModificationTime();
                lastName = name;
            }
        }
        fs.close();
        return lastName;
    }

    public static List<Object[]> findFilesSortDescByTime(String nameNode, String userName, String file, String containStr) throws Exception {
        FileSystem fs = getFs(nameNode, userName);
        FileStatus[] listStatus = fs.listStatus(new Path(file));

        List<Object[]> fileList = new ArrayList<>();
        for (FileStatus fstatus : listStatus) {
            String name = fstatus.getPath().getName();
            if (name.contains(containStr)) {
                Object[] temp = new Object[2];
                temp[0] = name;
                temp[1] = fstatus.getModificationTime();
                fileList.add(temp);
            }
        }
        fs.close();

        fileList.sort((o1, o2) ->
            ((Long) o1[1]) - ((Long) o2[1]) >= 0 ? -1 : 1
        );
        return fileList;
    }

    public static Object[] findFileSortDescByTime(String nameNode, String userName, String file, String containStr, int index) throws Exception {
        List<Object[]> fileList = findFilesSortDescByTime(nameNode, userName, file, containStr);
        if (fileList.size() < (index + 1)) {
            return null;
        }

        return fileList.get(index);
    }

    public static void deleteFiles(String nameNode, String userName, List<String> files) throws Exception {
        FileSystem fs = getFs(nameNode, userName);
        for (int i = 0; i < files.size(); i++) {
            Path path = new Path(files.get(i));
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        }
        fs.close();
    }

//    /**
//     * 在hfds中创建目录、删除目录、重命名
//     */
//    public void mkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {
//        // 创建目录
//        fs.mkdirs(new Path("/a1/b1/c1"));
//
//        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true
//        fs.delete(new Path("/aaa"), true);
//
//        // 重命名文件或文件夹
//        fs.rename(new Path("/a1"), new Path("/a2"));
//    }
//
//    /**
//     * 查看目录信息，只显示文件
//     */
//    public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
//        // 思考：为什么返回迭代器，而不是List之类的容器
//        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
//
//        while (listFiles.hasNext()) {
//            LocatedFileStatus fileStatus = listFiles.next();
//
//            System.out.println(fileStatus.getPath().getName());
//            System.out.println(fileStatus.getBlockSize());
//            System.out.println(fileStatus.getPermission());
//            System.out.println(fileStatus.getLen());
//            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
//            for (BlockLocation bl : blockLocations) {
//                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
//                String[] hosts = bl.getHosts();
//                for (String host : hosts) {
//                    System.out.println(host);
//                }
//            }
//            System.out.println("--------------分割线--------------");
//        }
//    }
//
//    /**
//     * 查看文件及文件夹信息
//     */
//    public void listAll() throws FileNotFoundException, IllegalArgumentException, IOException {
//        FileStatus[] listStatus = fs.listStatus(new Path("/"));
//        String flag = "d--             ";
//
//        for (FileStatus fstatus : listStatus) {
//            if (fstatus.isFile())
//                flag = "f--         ";
//            System.out.println(flag + fstatus.getPath().getName());
//        }
//    }
}
