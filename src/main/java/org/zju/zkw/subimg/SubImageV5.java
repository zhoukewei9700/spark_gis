package org.zju.zkw.subimg;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.NULL;
import org.apache.spark.storage.StorageLevel;
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


public class SubImageV5 {
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
//        String jsPath = JSON_PATH;
//        String mbtilesPath = MBTILES_DIR;
//        String mbtilesFolder = JSON_PATH;

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
        //logger.info(String.valueOf(allTif.size()));
        //创建RDD
        JavaRDD<String> allTifRDD = sc.parallelize(allTif).repartition(sc.defaultParallelism());
        allTifRDD.persist(StorageLevel.DISK_ONLY());
        //从高到低逐层切片
        for (int zoomLevel = maxZ; zoomLevel >= minZ; zoomLevel--) {
            //创建瓦片信息
            //final int zoomLevelf = zoomLevel;
            double dResolution = zResolution.get(zoomLevel);
            List<Tuple2<String, RasterInfo>> mapSubdivision = RasterProcess.createMapSubdivision(
                    new Extent(-20026376.39, 20026376.39, -20048966.10, 20048966.10),
                    zoomLevel, 256, 256, BAND_NUM, PROJECT, pngFolder);
            //创建一个hash表用来存sub的信息
            HashMap<String, Extent> subInfo = new HashMap< >();
            Driver memDriver = gdal.GetDriverByName("MEM");
            Driver pngDriver = gdal.GetDriverByName("PNG");
            for (Tuple2<String, RasterInfo> sub : mapSubdivision) {
                subInfo.put(sub._1, sub._2().getExtent());
                Dataset memDataset = memDriver.Create("", 256, 256, BAND_NUM, gdalconst.GDT_Byte);
                Dataset pngDataset = pngDriver.CreateCopy(sub._1, memDataset);
            }

            //判断是否重合并填充png
            //Connection finalConn = conn;
            allTifRDD.flatMapToPair(
                            (PairFlatMapFunction<String, String, String>) tif -> {
                                List<Tuple2<String, String>> overlaps = new ArrayList<>();
                                //logger.info("tif: "+tif);
                                Dataset imgSrc = gdal.Open(tif, gdalconst.GA_ReadOnly);//打开tif
                                //logger.info("Dataser imgSrc: "+imgSrc);
                                SpatialReference destSR = new SpatialReference(PROJECT);//先做投影变换，将投影转换成web墨卡托
                                CoordinateTransformation ct = new CoordinateTransformation(imgSrc.GetSpatialRef(), destSR);

                                for (Tuple2<String, RasterInfo> sub : mapSubdivision) {//和空白的切片分幅判断是否重合
                                    //logger.info("intersect or not: " + tif + " " + sub._1);
                                    if (RasterProcess.getExtent(imgSrc, ct).intersect(sub._2.getExtent())) {
                                        overlaps.add(new Tuple2<>(sub._1, tif));
                                    }
                                }
                                imgSrc.delete();
                                return overlaps.iterator();
                            }).groupByKey()
                    .map((Function<Tuple2<String, Iterable<String>>, String>) kv -> {
                        List<String> list = IteratorUtils.toList(kv._2.iterator());
                        //logger.info("start to fillsub: "+kv._1);
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

    }

    private static String fillSub(String sub, List<String> tifs, Extent extent, double dRes) {
        double dMapX, dMapY;  //
        int iDestX, iDestY;

        CoordinateTransformation ct = null;
        logger.info("start to fillsub: " + sub);
        int xSize = 256;
        int ySize = 256;
        int nBand = 3;

        //Dataset dsDest = gdal.Open(sub, gdalconst.GF_Write);  //打开瓦片准备写入

        int[][] tempImgArray = new int[BAND_NUM][65536];
        double[] GT = {extent.getMinX(), dRes, 0.0, extent.getMaxY(), 0.0, -dRes};
        for (String tif : tifs) {
            logger.info("====================start to read raster " + tif + "====================");
            Dataset dsSrc = gdal.Open(tif, gdalconst.GF_Read);
            int uiCols = dsSrc.GetRasterXSize();
            int uiRows = dsSrc.GetRasterYSize();
            int uiBands = dsSrc.GetRasterCount();
            SpatialReference srcSR = dsSrc.GetSpatialRef();
            SpatialReference destSR = new SpatialReference(PROJECT);//新投影为WEB墨卡托
            CoordinateTransformation ct2 = new CoordinateTransformation(destSR, srcSR);//从瓦片的坐标系转换到tif的坐标系
            double[] arrGeoTransform = new double[6];
            dsSrc.GetGeoTransform(arrGeoTransform);
            short[] imgArray = new short[1];
            //最邻近重采样
            for (int y = 0; y < ySize; y++) {
                for (int x = 0; x < xSize; x++) {
                    //计算png像素的地理坐标
                    double[] pngGeoXY = RasterProcess.imageXY2Geo(x, y, GT);
                    double[] tifGeoXY = ct2.TransformPoint(pngGeoXY[0], pngGeoXY[1]);//将坐标转换成tif坐标系下的坐标
                    int[] tifImageXY = RasterProcess.geo2ImageXY(tifGeoXY[0], tifGeoXY[1], arrGeoTransform);//计算在tif上的像素坐标
                    if (!(tifImageXY[0] < 0 || tifImageXY[0] > uiCols || tifImageXY[1] < 0 || tifImageXY[1] > uiRows)) {
                        dsSrc.ReadRaster(tifImageXY[0], tifImageXY[1], 1, 1, 1, 1, gdalconst.GDT_UInt16, imgArray, new int[]{1});
                        tempImgArray[0][y * 256 + x] = imgArray[0];
                    }
                }
            }
        }
        Driver memDriver = gdal.GetDriverByName("MEM");
        Dataset memDataset = memDriver.Create("", xSize, ySize, BAND_NUM, gdalconst.GDT_Byte);
        for (int i = 1; i <= BAND_NUM; i++) {
            byte[] imgArray2 = RasterProcess.compress(tempImgArray[i - 1], ySize, xSize, 0.02, 0.98);
            memDataset.WriteRaster(0, 0, xSize, ySize, xSize, ySize, gdalconst.GDT_Byte, imgArray2, new int[]{i});
        }
        //logger.info("====================Write into memDataset Finished====================");
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
