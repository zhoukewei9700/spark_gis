package org.zju.zkw.subimg;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.NULL;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;
import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zju.zkw.information.Extent;
import org.zju.zkw.information.RasterInfo;
import org.zju.zkw.information.ImageType;
import org.zju.zkw.rasterprocess.MbtilesUtils;
import org.zju.zkw.rasterprocess.RasterProcess;
import org.zju.zkw.rasterprocess.ShellCreate;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.zju.zkw.Constant.*;


public class SubImageV3 {
    private static final Logger logger = LoggerFactory.getLogger(SubImage.class);
    //wpsg:3857
    public static final String PROJECT = "PROJCRS[\"WGS 84 / Pseudo-Mercator\",\n" +
            "    BASEGEOGCRS[\"WGS 84\",\n" +
            "        DATUM[\"World Geodetic System 1984\",\n" +
            "            ELLIPSOID[\"WGS 84\",6378137,298.257223563,\n" +
            "                LENGTHUNIT[\"metre\",1]]],\n" +
            "        PRIMEM[\"Greenwich\",0,\n" +
            "            ANGLEUNIT[\"degree\",0.0174532925199433]],\n" +
            "        ID[\"EPSG\",4326]],\n" +
            "    CONVERSION[\"Popular Visualisation Pseudo-Mercator\",\n" +
            "        METHOD[\"Popular Visualisation Pseudo Mercator\",\n" +
            "            ID[\"EPSG\",1024]],\n" +
            "        PARAMETER[\"Latitude of natural origin\",0,\n" +
            "            ANGLEUNIT[\"degree\",0.0174532925199433],\n" +
            "            ID[\"EPSG\",8801]],\n" +
            "        PARAMETER[\"Longitude of natural origin\",0,\n" +
            "            ANGLEUNIT[\"degree\",0.0174532925199433],\n" +
            "            ID[\"EPSG\",8802]],\n" +
            "        PARAMETER[\"False easting\",0,\n" +
            "            LENGTHUNIT[\"metre\",1],\n" +
            "            ID[\"EPSG\",8806]],\n" +
            "        PARAMETER[\"False northing\",0,\n" +
            "            LENGTHUNIT[\"metre\",1],\n" +
            "            ID[\"EPSG\",8807]]],\n" +
            "    CS[Cartesian,2],\n" +
            "        AXIS[\"easting (X)\",east,\n" +
            "            ORDER[1],\n" +
            "            LENGTHUNIT[\"metre\",1]],\n" +
            "        AXIS[\"northing (Y)\",north,\n" +
            "            ORDER[2],\n" +
            "            LENGTHUNIT[\"metre\",1]],\n" +
            "    USAGE[\n" +
            "        SCOPE[\"unknown\"],\n" +
            "        AREA[\"World - 85°S to 85°N\"],\n" +
            "        BBOX[-85.06,-180,85.06,180]],\n" +
            "    ID[\"EPSG\",3857]]";

    static {
        gdal.AllRegister();
    }

    public static void main(String @NotNull [] args) throws SQLException, IOException, InterruptedException {
        logger.info("=======subimage start=======");
        //input,output
//        String tifFolder = "D:\\ZJU_GIS\\testpic\\pic2";
//        String pngFolder = "D:\\ZJU_GIS\\outPNG";
//        String zRange = "4-4";
//        String jsPath =" D:\\ZJU_GIS\\mbtile";
//        String mbtilesPath = "D:\\ZJU_GIS\\mbtile\\test.mbtiles";
//        String mbtilesFolder = "D:\\ZJU_GIS\\mbtile";
        String tifFolder = args[0];
        String pngFolder = args[1];
        String zRange = args[2];
        String jsPath = JSON_PATH;
        String mbtilesPath = MBTILES_DIR;
        String mbtilesFolder = JSON_PATH;

        File fileDir = new File(tifFolder);
        if (fileDir.isFile()) {
            logger.error("Input should be a directory,exit");
            System.exit(1);
        }
        //define zoom level
        String[] zRangeArr = zRange.split("-");
        if (zRangeArr.length != 2) {
            logger.error("zRange should be like 'z1-z2', z1 & z2 should be integer in 0-15!");
            System.exit(1);
        }
        int minZ = Integer.parseInt(zRangeArr[0]);
        int maxZ = Integer.parseInt(zRangeArr[1]);
        if (minZ > maxZ) {
            logger.error("zRange should be like 'z1-z2', z1 should be less than z2!");
            System.exit(1);
        }

        //spark setup
        SparkSession ss = SparkSession
                .builder()
                //.master("local[*]")
                .appName("subImage")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());

        //从高到低逐层切片
        for (int zoomLevel = maxZ; zoomLevel >= minZ; zoomLevel--) {
            //创建瓦片信息
            //final int zoomLevelf = zoomLevel;
            double dResolution = zResolution.get(zoomLevel);
            List<Tuple2<String, RasterInfo>> mapSubdivision = RasterProcess.createMapSubdivision(
                    new Extent(-20026376.39, 20026376.39, -20048966.10, 20048966.10),
                    zoomLevel, 256, 256, BAND_NUM, PROJECT, pngFolder);
            //创建一个hash表用来存sub的信息
            HashMap<String, Extent> subInfo = new HashMap<>();
            Driver memDriver = gdal.GetDriverByName("MEM");
            Driver pngDriver = gdal.GetDriverByName("PNG");
            Dataset memDataset;
            Dataset pngDataset = null;
            for (Tuple2<String, RasterInfo> sub : mapSubdivision) {
                memDataset = memDriver.Create("", 256, 256, 3, gdalconst.GDT_Byte);
                pngDataset = pngDriver.CreateCopy(sub._1, memDataset);
                subInfo.put(sub._1, sub._2().getExtent());
            }
            memDriver.delete();
            pngDataset.delete();

            //读取所有tif
            List<String> allTif = new ArrayList<>();
            for (String fileSubDir : Objects.requireNonNull(fileDir.list())) {
                String[] tifList = new File(tifFolder + "/" + fileSubDir).list();
                //String[] tifList = new File(tifFolder +"\\"+ fileSubDir).list();
                if (tifList == null) {
                    continue;
                }
                List<String> filter = Arrays.stream(Objects.requireNonNull(tifList))
                        .filter(f -> f.endsWith(".tif")).map(f -> tifFolder + "/" + fileSubDir + "/" + f).collect(Collectors.toList());
                //.filter(f -> f.endsWith(".tif")).map(f -> tifFolder +"\\"+ fileSubDir + "\\"+ f).collect(Collectors.toList());
                allTif.addAll(filter);
            }
            //创建RDD
            JavaRDD<String> allTifRDD = sc.parallelize(allTif).repartition(sc.defaultParallelism());


            //判断是否重合并填充png
            //Connection finalConn = conn;
            allTifRDD.flatMapToPair(
                            (PairFlatMapFunction<String, String, String>) tif -> {
                                List<Tuple2<String, String>> overlaps = new ArrayList<>();

                                Dataset imgSrc = gdal.Open(tif, gdalconst.GA_ReadOnly);//打开tif
                                SpatialReference destSR = new SpatialReference(PROJECT);//先做投影变换，将投影转换成web墨卡托
                                CoordinateTransformation ct = new CoordinateTransformation(imgSrc.GetSpatialRef(), destSR);

                                for (Tuple2<String, RasterInfo> sub : mapSubdivision) {//和空白的切片分幅判断是否重合
                                    logger.info("intersect or not: " + tif + " " + sub._1);
                                    if (RasterProcess.getExtent(imgSrc, ct).intersect(sub._2.getExtent())) {
                                        overlaps.add(new Tuple2<>(sub._1, tif));
                                    }
                                }
                                imgSrc.delete();
                                return overlaps.iterator();
                            }).groupByKey()
                    .map((Function<Tuple2<String, Iterable<String>>, String>) kv -> {
                        List<String> list = IteratorUtils.toList(kv._2.iterator());
                        logger.info("start to fillsub: "+kv._1);
                        Extent extent = subInfo.get(kv._1);
                        return fillSub(kv._1, list, extent, dResolution);
                        //return "1";
                    })
                    .foreach(sub -> {
                        logger.info("fillsub finished " + sub);

                    });
        }
        sc.stop();
        sc.close();
        ss.stop();
        ss.close();

        //导入mbtiles
//        String command = "mb-uitl --scheme=tms "+pngFolder+" "+mbtilesPath;
//        Runtime runtime = Runtime.getRuntime();
//        Process process = runtime.exec(new String[]{"/bin/sh","-c",command});
//        int res = process.waitFor();
//        if(res!=0){
//            logger.error("insert failed!");
//        }
    }

    private static String fillSub(String sub, List<String> tifs, Extent extent, double dRes) {
        double dMapX, dMapY;  //
        int iDestX, iDestY;
        short[] imgArray = new short[1];
        CoordinateTransformation ct = null;
        logger.info("start to fillsub: " + sub);
        int xSize = 256;
        int ySize = 256;
        int nBand = 3;

        //Dataset dsDest = gdal.Open(sub, gdalconst.GF_Write);  //打开瓦片准备写入
        int[][] tempImgArray = new int[BAND_NUM][65536];
        for (String tif : tifs) {
            Dataset dsSrc = gdal.Open(tif, gdalconst.GF_Read);
            int uiCols = dsSrc.GetRasterXSize();
            int uiRows = dsSrc.GetRasterYSize();
            int uiBands = dsSrc.GetRasterCount();

            SpatialReference srcSR = dsSrc.GetSpatialRef();
            SpatialReference destSR = new SpatialReference(PROJECT);//新投影为WEB墨卡托
            ct = new CoordinateTransformation(srcSR, destSR);

            double[] arrGeoTransform = new double[6];
            dsSrc.GetGeoTransform(arrGeoTransform);

            //筛选和tif相交的部分
            double[] subLT = {extent.getMinX(), extent.getMaxY()};
            double[] subRB = {extent.getMaxX(), extent.getMinY()};
            CoordinateTransformation ct2 = new CoordinateTransformation(destSR,srcSR);//从瓦片的坐标系转换到tif的坐标系
            double[] subLT_trans = ct2.TransformPoint(subLT[0],subLT[1]);
            double[] subRB_trans = ct2.TransformPoint(subRB[0],subRB[1]);
            int[] subLT_ImageXY = RasterProcess.geo2ImageXY(subLT_trans[0],subLT_trans[1],arrGeoTransform);
            int[] subRB_ImageXY = RasterProcess.geo2ImageXY(subRB_trans[0],subRB_trans[1],arrGeoTransform);

            //判断是否越过原图像边界
//            if(subLT_ImageXY[0]>=0&&subLT_ImageXY[0]<=uiCols){
//                startCol = subLT_ImageXY[0];
//            }
//            if(subRB_ImageXY[0]<=uiCols&&subRB_ImageXY[0]>=0){
//                endCol = subRB_ImageXY[0];
//            }
//            if(subLT_ImageXY[1]>=0&&subLT_ImageXY[1]<=uiRows){
//                startRow = subLT_ImageXY[1];
//            }
//            if(subRB_ImageXY[1]<=uiRows&&subRB_ImageXY[1]>=0){
//                endRow = subRB_ImageXY[1];
//            }

            //resample
//            for (int y = 0; y<uiRows;y++){
//                for(int x=0;x<uiCols;x++){
            int startRow = Math.max(0,subLT_ImageXY[1]);
            int startCol = Math.max(0,subLT_ImageXY[0]);
            int endRow = Math.min(uiRows,subRB_ImageXY[1]+1);
            int endCol = Math.min(uiCols,subRB_ImageXY[0]+1);
            for (int y = startRow; y < endRow; y++) {
                for (int x = startCol; x < endCol; x++) {
                    // tif
                    //logger.info("==========start to transform==========");
                    double[] doubles = RasterProcess.imageXY2Geo(x, y, arrGeoTransform);
                    dMapX = doubles[0];
                    dMapY = doubles[1];

                    // 坐标转换 src->dest
                    double[] xyTransformed = ct.TransformPoint(dMapX, dMapY);
                    assert xyTransformed != null;
                    dMapX = xyTransformed[0];
                    dMapY = xyTransformed[1];

                    // subfill
                    double[] GT = {extent.getMinX(), dRes, 0.0, extent.getMaxY(), 0.0, -dRes};
                    int[] ints = RasterProcess.geo2ImageXY(dMapX, dMapY, GT);
                    iDestX = ints[0];
                    iDestY = ints[1];
                    //logger.info("===========start to read raster==========");
                    if (!(iDestX < 0 || iDestX >= 256 //判断是否超出瓦片边界
                            || iDestY < 0 || iDestY >= 256)) {
                        for (int iBand = 1; iBand <= uiBands; iBand++) {
                            // fill-in

                            dsSrc.ReadRaster(x, y, 1, 1, 1, 1, gdalconst.GDT_UInt16, imgArray, new int[]{iBand});
                            //if(imgArray[0]!=0){System.out.println("the pixel value is "+imgArray[0]+"  written to "+sub);}
                            if (imgArray[0] > 0) {
                                tempImgArray[iBand - 1][256 * iDestY + iDestX] = imgArray[0];//将所有数值插入临时数组
                            }
                        }
                    }
                    //logger.info("===========read raster finished==========");
                }
            }
            logger.info("====================Insert into tempArray Finished====================");
            dsSrc.delete();
        }
        Driver memDriver = gdal.GetDriverByName("MEM");
        Dataset memDataset = memDriver.Create("", xSize, ySize, BAND_NUM, gdalconst.GDT_Byte);
        for (int i = 1; i <= BAND_NUM; i++) {
            byte[] imgArray2 = RasterProcess.compress(tempImgArray[i - 1], ySize, xSize, 0.02, 0.98);
            memDataset.WriteRaster(0, 0, xSize, ySize, xSize, ySize, gdalconst.GDT_Byte, imgArray2, new int[]{i});
        }
        logger.info("====================Write into memDataset Finished====================");
        Driver pngDriver = gdal.GetDriverByName("PNG");
        Dataset pngDataset = pngDriver.CreateCopy(sub, memDataset);
        logger.info("====================Createcopy Finished====================");
        if (pngDataset == null)
            return "fillsub failed!";
        else {
            pngDataset.delete();
            memDataset.delete();

        }
        logger.info("Finished");
        //dsDest.FlushCache();
        //dsDest.delete();
        return sub;
    }



    private static String removeExtension(String fName) {

        int pos = fName.lastIndexOf('.');
        if (pos > -1)
            return fName.substring(0, pos);
        else
            return fName;
    }
}
