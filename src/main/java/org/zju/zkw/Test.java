package org.zju.zkw;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
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
import org.zju.zkw.rasterprocess.RasterProcess;
import org.zju.zkw.subimg.SubImage;
import scala.Tuple2;
import static org.zju.zkw.Constant.*;
import javax.validation.constraints.NotNull;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.sqlite.JDBC;
//import com.github.jaiimageio.*;
public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);
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
    public static void main(String[] args) throws IOException, SQLException {

        Connection conn = null;
        try {
            // db parameters
            Class.forName("org.sqlite.JDBC");
            //String url = "jdbc:sqlite:/root/zkw/mbtiles/test.mbtiles";
            String url = "jdbc:sqlite:D:\\ZJU_GIS\\mbtile\\test.mbtiles";
            // create a connection to the database
            conn = DriverManager.getConnection(url);

            System.out.println("Connection to SQLite has been established.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }



//        File f = new File("D:\\ZJU_GIS\\OutImg\\4\\1\\0.TIF");
//        File pre = f.getParentFile().getParentFile().getParentFile();
//        File finalFile = new File("D:\\ZJU_GIS\\outPNG"+"\\"+f.toString().replace(pre.toString(),""));
//        String fName = removeExtension(finalFile.getName());
//        String pngOut = finalFile.getParent()+"\\"+fName+".png";


//    String tif="D:\\ZJU_GIS\\testpic\\pic1\\LT05\\LT05_L1TP_119039_20110730_20161007_01_T1_B1.TIF";
//    Dataset dst = gdal.Open(tif);
//    //int band=1;
//    for(int i=0;i<dst.getRasterYSize();i++){
//        for(int j=0;j<dst.getRasterXSize();j++){
//            short[] imgArray = new short[1];
//            for(int band=1;band<=dst.getRasterCount();band++) {
//                dst.ReadRaster(i, j, 1, 1, 1, 1, gdalconst.GDT_UInt16, imgArray, new int[]{band});
//                if(imgArray[0]!=0){
//                    System.out.print(imgArray[0]+" ");
//                }
//
//            }
//        }
//        System.out.println();
//    }
//        SparkSession ss = SparkSession
//                .builder()
//                .master("local[*]")
//                .appName("subImage")
//                .getOrCreate();
//        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
//        String tifFolder = "D:\\ZJU_GIS\\testpic\\pic1";
//        File fileDir = new File(tifFolder);
//        if(fileDir.isFile()){
//            logger.error("Input should be a directory,exit");
//            System.exit(1);
//        }
//        List<Tuple2<String, RasterInfo>> mapSubdivision = RasterProcess.createMapSubdivision(
//                new Extent(-20026376.39, 20026376.39, -20048966.10, 20048966.10),
//                4,256,256,BAND_NUM,PROJECT,"D:\\ZJU_GIS\\OutImg");
//        List<String> allTif = new ArrayList<>();
//        for(String fileSubDir: Objects.requireNonNull(fileDir.list())) {
//            String[] tifList = new File(tifFolder +"\\"+ fileSubDir).list();
//            if (tifList == null) {
//                continue;
//            }
//            List<String> filter = Arrays.stream(Objects.requireNonNull(tifList))
//                    .filter(f -> f.endsWith(".TIF")).map(f -> tifFolder +"\\"+ fileSubDir + "\\"+ f).collect(Collectors.toList());
//            allTif.addAll(filter);
//        }
//        //创建RDD
//        JavaRDD<String> allTifRDD = sc.parallelize(allTif).repartition(sc.defaultParallelism());
//        allTifRDD.flatMapToPair(
//                        (PairFlatMapFunction<String,String,String>) tif->{
//                            List<Tuple2<String,String>> overlaps = new ArrayList<>();
//
//                            Dataset imgSrc = gdal.Open(tif,gdalconst.GA_ReadOnly);//打开tif
//                            SpatialReference destSR = new SpatialReference(PROJECT);//先做投影变换，将投影转换成web墨卡托
//                            CoordinateTransformation ct = new CoordinateTransformation(imgSrc.GetSpatialRef(),destSR);
//
//                            for(Tuple2<String,RasterInfo> sub:mapSubdivision){//和空白的切片分幅判断是否重合
//                                logger.info("intersect or not: "+tif+ " "+sub._1 );
//                                if(RasterProcess.getExtent(imgSrc,ct).intersect(sub._2.getExtent())){
//                                    overlaps.add(new Tuple2<>(sub._1,tif));
//                                }
//                            }
//                            imgSrc.delete();
//                            return overlaps.iterator();
//                        }).groupByKey()
//                .map((Function<Tuple2<String, Iterable<String>>,String>) kv -> {
//                    List<String> list = IteratorUtils.toList(kv._2.iterator());
//                    logger.info("fill map: " + kv._1 + " " + list);
//                    return fillSub(kv._1,list);
//                    //return "1";
//                })
//                .foreach(logger::error);


    }

    private static String removeExtension(String fName) {

        int pos = fName.lastIndexOf('.');
        if(pos > -1)
            return fName.substring(0, pos);
        else
            return fName;
    }
    private static String fillSub(String sub,List<String> tifs) {
        double dMapX, dMapY;  //
        int iDestX, iDestY;
        short[] imgArray = new short[1];
        CoordinateTransformation ct = null;

        Dataset dsDest = gdal.Open(sub, gdalconst.GF_Write);  //打开瓦片准备写入

        for (String tif : tifs) {
            Dataset dsSrc = gdal.Open(tif, gdalconst.GF_Read);
            int uiCols = dsSrc.GetRasterXSize();
            int uiRows = dsSrc.GetRasterYSize();
            int uiBands = dsSrc.GetRasterCount();

            SpatialReference srcSR = dsSrc.GetSpatialRef();
            SpatialReference destSR = dsDest.GetSpatialRef();
            ct = new CoordinateTransformation(srcSR, destSR);


            double[] arrGeoTransform = new double[6];
            dsSrc.GetGeoTransform(arrGeoTransform);

            //resample
            for (int y = 0; y < uiRows; y++) {
                for (int x = 0; x < uiCols; x++) {
                    // tif
                    double[] doubles = RasterProcess.imageXY2Geo(x, y, arrGeoTransform);
                    dMapX = doubles[0];
                    dMapY = doubles[1];

                    // 坐标转换 src->dest
                    double[] xyTransformed = ct.TransformPoint(dMapX, dMapY);
                    assert xyTransformed != null;
                    dMapX = xyTransformed[0];
                    dMapY = xyTransformed[1];

                    // subfile
                    int[] ints = RasterProcess.geo2ImageXY(dMapX, dMapY, dsDest.GetGeoTransform());
                    iDestX = ints[0];
                    iDestY = ints[1];

                    if (!(iDestX < 0 || iDestX >= dsDest.GetRasterXSize() //判断是否超出瓦片边界
                            || iDestY < 0 || iDestY >= dsDest.GetRasterYSize())) {
                        for (int iBand = 1; iBand <= uiBands; iBand++) {
                            // fill-in
                            dsSrc.ReadRaster(x, y, 1, 1, 1, 1, gdalconst.GDT_UInt16, imgArray, new int[]{iBand});
                            //if(imgArray[0]!=0){System.out.println("the pixel value is "+imgArray[0]+"  written to "+sub);}
                            if(imgArray[0]>0){
                                logger.info("write "+x+","+y+"  to  "+iDestX+","+iDestY);
                                dsDest.WriteRaster(iDestX, iDestY, 1, 1, 1, 1, gdalconst.GDT_UInt16, imgArray, new int[]{iBand});
                            }
                            //dsDest.WriteRaster(iDestX, iDestY, 1, 1, 1, 1, gdalconst.GDT_UInt16, imgArray, new int[]{iBand});
                        }
                    }
                }
            }

            dsSrc.delete();
        }
        dsDest.FlushCache();
        dsDest.delete();
        return "Finished";
    }
}
