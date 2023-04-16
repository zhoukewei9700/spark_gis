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
import javax.xml.crypto.Data;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
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
        String tif = "D:\\ZJU_GIS\\testpic\\pic3\\2015\\GLCFCS30_E120N35.tif";
        Dataset dsSrc = gdal.Open(tif,gdalconst.GF_Read);

        CoordinateTransformation ct = null;
        int uiCols = dsSrc.GetRasterXSize();
        int uiRows = dsSrc.GetRasterYSize();
        int uiBands = dsSrc.GetRasterCount();
        short[] imgArray = new short[uiCols*uiRows];
        double dMapX, dMapY;  //
        int iDestX, iDestY;
        SpatialReference srcSR = dsSrc.GetSpatialRef();
        SpatialReference destSR = new SpatialReference(PROJECT);//新投影为WEB墨卡托
        ct = new CoordinateTransformation(srcSR, destSR);

        double[] arrGeoTransform = new double[6];
        dsSrc.GetGeoTransform(arrGeoTransform);
        Date startdate = new Date();
        System.out.println(startdate);
        dsSrc.ReadRaster(0,0,uiCols,uiRows,uiCols,uiRows,gdalconst.GDT_UInt16,imgArray,new int[]{1});
        for(int y=0;y<uiRows;y++){
            for(int x=0;x<uiCols;x++){
                if(imgArray[y*uiCols+x]>0) {
                    System.out.print(x + " " + y);break;
                }
            }
            System.out.println("");
        }
        Date enddate = new Date();
        System.out.println(enddate);
    }

    private static String removeExtension(String fName) {

        int pos = fName.lastIndexOf('.');
        if(pos > -1)
            return fName.substring(0, pos);
        else
            return fName;
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
            int startCol=0;
            int startRow=0;
            int endCol = uiCols;
            int endRow = uiRows;
            //判断是否越过原图像边界
            if(subLT_ImageXY[0]>=0&&subLT_ImageXY[0]<=uiCols){
                startCol = subLT_ImageXY[0];
            }
            if(subRB_ImageXY[0]<=uiCols&&subRB_ImageXY[0]>=0){
                endCol = subRB_ImageXY[0];
            }
            if(subLT_ImageXY[1]>=0&&subLT_ImageXY[1]<=uiRows){
                startRow = subLT_ImageXY[1];
            }
            if(subRB_ImageXY[1]<=uiRows&&subRB_ImageXY[1]>=0){
                endRow = subRB_ImageXY[1];
            }

            //resample
//            for (int y = 0; y<uiRows;y++){
//                for(int x=0;x<uiCols;x++){
            for (int y = startRow; y <= endRow; y++) {
                for (int x = startCol; x <= endCol; x++) {
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
}
