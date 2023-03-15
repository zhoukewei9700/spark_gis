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
import org.zju.zkw.rasterprocess.RasterProcess;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.zju.zkw.Constant.*;

public class SubImage {

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

    public static void main(String @NotNull [] args){


        logger.info("=======subimage start=======");

        //input,output
        String tifFolder = args[0];
        String subOut = SUB_FOLDER;
        String subFolder = SUB_FOLDER;
        String pngFolder = args[1];
        String jsonPath = JSON_PATH;
        String mbtilesPath = MBTILES_DIR;
        String mbutilPath = MBUTIL_DIR;
        String zRange = args[2];

        File fileDir = new File(tifFolder);
        if(fileDir.isFile()){
            logger.error("Input should be a directory,exit");
            System.exit(1);
        }

        String[] zRangeArr = zRange.split("-");
        if(zRangeArr.length !=2){
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

        //创建空白的分幅
        //由高到低逐层切
        for(int zoomLevel = maxZ;zoomLevel>=minZ;zoomLevel--){
            List<Tuple2<String, RasterInfo>> mapSubdivision = RasterProcess.createMapSubdivision(
                    new Extent(-20026376.39, 20026376.39, -20048966.10, 20048966.10),
                    zoomLevel,256,256,BAND_NUM,PROJECT,subOut);

            JavaPairRDD<String, RasterInfo> subdivisionRDD = sc.parallelizePairs(mapSubdivision).repartition(sc.defaultParallelism());
            subdivisionRDD.foreach(sub -> {
                RasterProcess.createImage(sub._1, sub._2, ImageType.TIF, gdalconst.GDT_UInt16);
                logger.info("create image " + sub._1);
            });
            List<String> allTif = new ArrayList<>();
            for(String fileSubDir: Objects.requireNonNull(fileDir.list())) {
                String[] tifList = new File(tifFolder +"/"+ fileSubDir).list();
                if (tifList == null) {
                    continue;
                }
                List<String> filter = Arrays.stream(Objects.requireNonNull(tifList))
                        .filter(f -> f.endsWith(".tif")).map(f -> tifFolder +"/"+ fileSubDir + "/"+ f).collect(Collectors.toList());
                allTif.addAll(filter);
            }
            //创建RDD
            JavaRDD<String> allTifRDD = sc.parallelize(allTif).repartition(sc.defaultParallelism());
            /***
             * JavaPairRDD<K2,V2> flatMapToPair(PairFlatMapFunction<T,K2,V2> f)
             * 此函数对对一个RDD中的每个元素(每个元素都是T类型的)调用f函数
             * 通过f函数可以将每个元素转换为<K2,V2>类型的元素
             * 比mapToPair方法多了一个flat操作，将所有的<K2,V2>类型的元素合并成为一个Iterable<Tuple2<K2, V2>>类型的对象。
             */
            allTifRDD.flatMapToPair(
                    (PairFlatMapFunction<String,String,String>)tif->{
                        List<Tuple2<String,String>> overlaps = new ArrayList<>();

                        Dataset imgSrc = gdal.Open(tif,gdalconst.GA_ReadOnly);//打开tif
                        SpatialReference destSR = new SpatialReference(PROJECT);//先做投影变换，将投影转换成web墨卡托
                        CoordinateTransformation ct = new CoordinateTransformation(imgSrc.GetSpatialRef(),destSR);

                        for(Tuple2<String,RasterInfo> sub:mapSubdivision){//和空白的切片分幅判断是否重合
                            logger.info("intersect or not: "+tif+ " "+sub._1 );
                            if(RasterProcess.getExtent(imgSrc,ct).intersect(sub._2.getExtent())){
                                overlaps.add(new Tuple2<>(sub._1,tif));
                            }
                        }
                        imgSrc.delete();
                        return overlaps.iterator();
                    }).groupByKey()
                    .map((Function<Tuple2<String, Iterable<String>>,String>) kv -> {
                        List<String> list = IteratorUtils.toList(kv._2.iterator());
                        //logger.info("start to fillsub: "+kv._1);
                        return fillSub(kv._1,list);
                        //return "1";
                    })
                    .foreach(logger::error);

            //TIF转PNG
            List<String> allSubs = new ArrayList<>();

            File pngDir = new File(subFolder);
            if(pngDir.isFile()){
                logger.error("Input should be a directory,exit");
                System.exit(1);
            }
            //栅格图像目录下的所有tif文件
            for(String fileSubDir: Objects.requireNonNull(pngDir.list())) {
                File file = new File(pngFolder+"/"+fileSubDir);
                if(!file.exists()){
                    file.mkdirs();
                }
                for (String yList : Objects.requireNonNull(new File(subFolder +"/"+ fileSubDir).list())) {
                    File file2 = new File(pngFolder+"/"+fileSubDir+"/"+yList);
                    if(!file2.exists()){
                        file2.mkdirs();
                    }
                    String[] pngList = new File(subFolder +"/"+ fileSubDir +"/"+ yList).list();
                    if (pngList == null) {
                        continue;
                    }
                    List<String> filter = Arrays.stream(Objects.requireNonNull(pngList))
                            .filter(f -> f.endsWith(".tif")).map(f -> subFolder +"/"+ fileSubDir +"/"+ yList + "/" + f).collect(Collectors.toList());
                    allSubs.addAll(filter);
                }
            }
            JavaRDD<String> allSubRDD = sc.parallelize(allSubs).repartition(sc.defaultParallelism());
            allSubRDD.foreach(sub -> {
                File f = new File(sub);
                File pre = f.getParentFile().getParentFile().getParentFile();
                File finalFile = new File(pngFolder+"/"+f.toString().replace(pre.toString(),""));
                String fName = removeExtension(finalFile.getName());
                String pngOut = finalFile.getParent()+"/"+fName+".png";
                Dataset tifDataset = gdal.Open(sub,gdalconst.GF_Read);
                boolean isConvertSuccess = RasterProcess.convertTIF2PNG(pngOut,tifDataset);
                if(!isConvertSuccess){
                    logger.error("failed when converting "+sub+" to png");
                }

            });
        }
        //生成mbtiles

        File jsonFile = new File(jsonPath+"/"+"metadata.json");
        if(!jsonFile.exists()){
            try{
                jsonFile.createNewFile();
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }
        //获取坐标
//        int nRow = ds.getRasterYSize();
//        int nCol = ds.getRasterXSize();
//        double[] coordLB = RasterProcess.imageXY2Geo(ds,0,nRow);//左下
//        double[] LB = RasterProcess.pro2geo(ds,coordLB[0],coordLB[1]);
//
//        double[] coordRT = RasterProcess.imageXY2Geo(ds,nCol,0);//右上
//        double[] RT = RasterProcess.pro2geo(ds,coordRT[0],coordRT[1]);
        String version = "\"1.2\"";
        String bounds = "\""+"20026376.39"+","+"-20048966.10"+","+"20026376.39"+","+"20048966.10"+"\"";
        String description = "\"sub pic\"";
        String json = "{\"version\":"+version+","+"\"bounds\":"+bounds+",\"description\":"+description
                +",\"format\":\"png\"}";
        try{
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jsonFile),"UTF-8"));
            out.write(json);
            out.flush();
            out.close();
        }catch (Exception e){
            e.printStackTrace();
        }

        callCMD(pngFolder,mbtilesPath,mbutilPath);
    }


    /***
     *
     * @param sub：瓦片名
     * @param tifs：每张瓦片对应的所有需要处理的图像
     * @return ：完成的信号
     */
    private static String fillSub(String sub,List<String> tifs) {
        double dMapX, dMapY;  //
        int iDestX, iDestY;
        short[] imgArray = new short[1];
        CoordinateTransformation ct = null;
        logger.info("start to fillsub: "+sub);
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
                                dsDest.WriteRaster(iDestX, iDestY, 1, 1, 1, 1, gdalconst.GDT_UInt16, imgArray, new int[]{iBand});
                            }

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
    /***
     * 返回文件的名称，不包括扩展名
     */
    private static String removeExtension(String fName) {

        int pos = fName.lastIndexOf('.');
        if(pos > -1)
            return fName.substring(0, pos);
        else
            return fName;
    }

    private static void callCMD(String tileDir,String mbtDir,String mbutilDir){
        try{
            String cmd = "mb-util --scheme=tms "+tileDir+" "+mbtDir;
            File dir = new File(mbutilDir);
            logger.info("start to running mbutil");
            String res=RasterProcess.exec(cmd);
            logger.info(res);
//            logger.info("CMD IS: "+"mb-util --scheme=tms "+tileDir+" "+mbtDir);
//            Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",cmd});
//            int status = process.waitFor();
//            if(status!=0){
//                System.err.println("Failed to call shell's command and the return status's is: " + status);
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
