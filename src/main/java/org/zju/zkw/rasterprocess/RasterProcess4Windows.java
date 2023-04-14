package org.zju.zkw.rasterprocess;

import org.zju.zkw.Constant;
import org.zju.zkw.information.*;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;
import org.gdal.ogr.Geometry;
import org.gdal.ogr.ogr;
import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;
import org.zju.zkw.subimg.SubImageV2;
import scala.Tuple2;

import javax.validation.ValidationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Stream;


public class RasterProcess4Windows {

    /***
     * 获取投影坐标系和地理坐标
     * 先有地理坐标系才能有投影坐标系，所以根据投影坐标系可以逆推出地理坐标系
     * @param dataset :raster dataset
     * @return projection system and geology system
     */
    public static SpatialReference[] getSRSPair(Dataset dataset){
        SpatialReference proSrs = new SpatialReference(dataset.GetProjection());//投影坐标系
        SpatialReference geoSrs = proSrs.CloneGeogCS();//地理坐标系
        return new SpatialReference[]{proSrs,geoSrs};
    }

    /***
     * @param dataset: raster dataset
     * @param projX: projection X
     * @param projY: projection Y
     * @return longitude,latitude
     */
    public static double[] pro2geo(Dataset dataset,double projX,double projY){
        SpatialReference srs[] = getSRSPair(dataset);
        CoordinateTransformation coordinateTransformation = new CoordinateTransformation(srs[0],srs[1]);
        return coordinateTransformation.TransformPoint(projX,projY);
    }

    /***
     * @param dataset: raster dataset
     * @param geoX: longitude
     * @param geoY: latitude
     * @return projection X,projection Y
     */
    public static double[] geo2pro(Dataset dataset,double geoX,double geoY){
        SpatialReference srs[] = getSRSPair(dataset);
        CoordinateTransformation coordinateTransformation = new CoordinateTransformation(srs[1],srs[0]);
        return coordinateTransformation.TransformPoint(geoX,geoY);
    }

    /***
     * //找到遥感影像中的点的实际坐标
     * @param dataset :raster dataset
     * @param col :pixel column index
     * @param row :pixel row index
     * @return longitude,latitude or projection coordinates
     */
    public static double[] imageXY2Geo(Dataset dataset,int col,int row){
        return imageXY2Geo(col,row,dataset.GetGeoTransform());
    }
    public static double[] imageXY2Geo(int col,int row,double[] GT){
        double longitude = GT[0]+col*GT[1]+row*GT[2];
        double latitude = GT[3]+col*GT[4]+row*GT[5];
        return new double[]{longitude,latitude};
    }

    /***
     * //找到实际坐标会在哪个像素点
     */
    public static int[] geo2ImageXY(Dataset dataset,double x,double y){
        return geo2ImageXY(x,y, dataset.GetGeoTransform());
    }
    public static int[] geo2ImageXY(double x,double y,double[] GT){
        int row = (int) ((x*GT[5]-y*GT[2]+GT[2]*GT[3]-GT[0]*GT[5])/(GT[1]*GT[5]-GT[4]*GT[2]));
        int col = (int) ((x*GT[4]-y*GT[1]-GT[0]*GT[4]+GT[1]*GT[3])/(GT[2]*GT[4]-GT[1]*GT[5]));
        return new int[]{row,col};
    }

    public static Extent getExtent(Dataset dataset,CoordinateTransformation ct){
        return getExtent(dataset.getRasterYSize(),dataset.getRasterXSize(),dataset.GetGeoTransform(),ct);
    }
    public static Extent getExtent(int row,int col,double GT[],CoordinateTransformation ct){
        double[] pLT = imageXY2Geo(0,0,GT);
        double[] pLB = imageXY2Geo(0,row,GT);
        double[] pRT = imageXY2Geo(col,0,GT);
        double[] pRB = imageXY2Geo(col,row,GT);

        if(ct!=null){//坐标转换
            pLT = ct.TransformPoint(pLT[0],pLT[1]);
            pLB = ct.TransformPoint(pLB[0],pLB[1]);
            pRT = ct.TransformPoint(pRT[0],pRT[1]);
            pRB = ct.TransformPoint(pRB[0],pRB[1]);
        }

        double minX = Math.min(pLT[0],pLB[0]);
        double minY = Math.min(pLB[1],pRB[1]);
        double maxX = Math.max(pRT[0],pRB[0]);
        double maxY = Math.max(pLT[1],pRT[1]);

        return new Extent(minX,maxX,minY,maxY);

    }
    public static void createImage(String out, RasterInfo rasterInfo, ImageType imageType, int eType) {

        Driver driver = null;

        switch (imageType) {
            case TIF:
                driver = gdal.GetDriverByName("GTiff");
                break;
            case PNG:
            default:
                throw new ValidationException("can not support this image type");
        }

        if (driver == null) {
            throw new ValidationException("can not find this driver");
        }

        Dataset create = driver.Create(out, rasterInfo.getCols(), rasterInfo.getRows(), rasterInfo.getBandNum(), eType);
        if (create == null) {
            throw new RuntimeException("create failed.");
        }

        create.SetProjection(rasterInfo.getProjectInfo().getProjWKT());
        create.SetGeoTransform(rasterInfo.getProjectInfo().getArrGeoTransform());

        driver.delete();
        create.delete();

    }
    public static List<Tuple2<String,RasterInfo>>createMapSubdivision(Extent extent,int zoomLevel, int subWidth,int subHeight,
                                                                      int bandNum,String destProj,String outFolder){
        //获取新影像仿射系数及行列数
        double dResolution = Constant.zResolution.get(zoomLevel);
        double[] GT = {extent.getMinX(), dResolution, 0.0, extent.getMaxY(), 0.0, -dResolution};
        double d1 = GT[1] * GT[5];
        double d2 = GT[2] * GT[4];
        int totalCols, totalRows;
        totalCols = (int) ((GT[5] * extent.getMaxX() - GT[2] * extent.getMinY() -
                GT[0] * GT[5] + GT[2] * GT[3]) / (d1 - d2)) + 1;
        totalRows = (int) ((GT[4] * extent.getMaxX() - GT[1] * extent.getMinY() -
                GT[0] * GT[4] + GT[1] * GT[3]) / (d2 - d1)) + 1;

        //double[] GT = {extent.getMinX(),dResolution,0.0, extent.getMaxY(),0.0,-dResolution};//仿射系数


        //计算分块数目
        int subRows = (int)Math.ceil(totalRows*1.0/subHeight);
        int subCols = (int)Math.ceil(totalCols*1.0/subWidth);

        //写出空白影像信息（*不满要求的行/列宽按照原始图像输出）
        RasterInfo subInfo = new RasterInfo()
                .setProjectInfo(new ProjectInfo()
                        .setProjWKT(destProj).setIGCPNum(0).setArrGeoTransform(GT.clone()))
                .setBandNum(bandNum)
                .setGdalType(gdalconst.GDT_Float32)
                .setCols(subWidth)
                .setRows(subHeight);
        String strImg;
        double dMapX,dMapY;

        List<Tuple2<String,RasterInfo>> subMaps = new ArrayList<>();
        for(int i=0;i<subRows;i++){
            for(int j=0;j<subCols;j++){
                double[] maps = imageXY2Geo(j*subWidth,i*subHeight,GT);//找到每个像素点对应的地理坐标
                dMapX = maps[0];
                dMapY = maps[1];

                //每个分幅变换
                double[] GT2 = subInfo.getProjectInfo().getArrGeoTransform();
                GT2[0]=dMapX;
                GT2[3]=dMapY;
                subInfo.getProjectInfo().setArrGeoTransform(GT2);
                subInfo.setExtent(getExtent(subHeight,subWidth,GT2,null));
                //File file = new File(outFolder+"/"+String.format("%d/%d",zoomLevel,j));
                File file = new File(outFolder+"\\"+String.format("%d\\%d",zoomLevel,j));
                if(!file.exists()){
                    if(!file.mkdirs()){
                        System.out.println("创建文件夹失败！");
                    }
                }

                //strImg = file.getPath()+"/"+String.format("%d.png",(int)(Math.pow(2,zoomLevel))-i);
                strImg = file.getPath()+"\\"+String.format("%d.png",(int)(Math.pow(2,zoomLevel))-i);
                //strImg = file.getPath()+"\\"+String.format("%d.tif",16-i);
//                strImg = outFolder+"\\"+String.format("%d\\%d\\%d.TIF",zoomLevel,j,16-i);
                subMaps.add(new Tuple2<>(strImg,subInfo.deepCopy()));

            }
        }
        return subMaps;
    }

    /***
     *
     * @param pngOut png图像输出路径
     * @param tifDataset 栅格数据集
     * @return 生成png是否成功
     */
    public static boolean convertTIF2PNG(String pngOut,Dataset tifDataset){
        int xSize= tifDataset.getRasterXSize();
        int ySize = tifDataset.getRasterYSize();
        int nBand = tifDataset.getRasterCount();
        int[][] imgArray = new int[nBand][xSize*ySize];
        Driver memDriver = gdal.GetDriverByName("MEM");
        Dataset memDataset = memDriver.Create("",xSize,ySize,nBand,gdalconst.GDT_Byte);
        for(int i=1;i<=nBand;i++){
            tifDataset.ReadRaster(0,0,xSize,ySize,xSize,ySize,gdalconst.GDT_Int32,imgArray[i-1],new int[]{i});

            //百分比截断压缩
            byte[] imgArray2 = compress(imgArray[i-1],ySize,xSize,0.02,0.98);

            memDataset.WriteRaster(0,0,xSize,ySize,xSize,ySize,gdalconst.GDT_Byte,imgArray2,new int[]{i});
            //System.out.println(Arrays.toString(imgArray[i-1]));
        }
        Driver pngDriver = gdal.GetDriverByName("PNG");
        Dataset pngDataset = pngDriver.CreateCopy(pngOut,memDataset);
        if(pngDataset==null)
            return false;
        else{
            pngDataset.delete();
            return true;
        }
    }

    /***
     * notice: ReadRaster先列再行读
     * @param tifArray 16位图像数据
     * @param row 行数
     * @param col 列数
     * @param perMin 最低截断值
     * @param perMax 最高截断值
     * @return 8位图像数据
     */
    public static byte[] compress(int[] tifArray,int row,int col,double perMin,double perMax){
        short[] pngArray= new short[row*col];
        byte[] byteArray = new byte[row*col];
        //累计直方图统计
        int[] temp= tifArray.clone();
        int cutMax = percentile(temp,perMax);
        int cutMin = percentile(temp,perMin);
        int bandMin = temp[0];
        int bandMax = temp[temp.length-1];
        double compressScale = (double)(cutMax-cutMin)/255;
        for(int i = 0;i<row;i++){
            for(int j=0;j<col;j++){
                if(tifArray[i*row+j]<cutMin){
                    pngArray[i*row+j] = 0;continue;
                }
                if(tifArray[i*row+j]>cutMax){
                    pngArray[i*row+j] = 255;continue;
                }
                pngArray[i*row+j] = (short)((tifArray[i*row+j]-cutMin)/compressScale);
                byteArray[i*row+j] = (pngArray[i*row+j]>127)?(byte)(pngArray[i*row+j]-256):(byte)pngArray[i*row+j];
            }
        }
        return byteArray;
    }

    /***
     *  取百分位数
     * @param data 16位数据数组
     * @param p 百分比
     * @return 百分位数
     */
    public static int percentile(int[] data,double p){
        int n = data.length;
        Arrays.sort(data);
        double x = (n-1)*p;
        int i = (int)(Math.floor(x));
        double g = x-i;
        if(g==0){
            return data[i];
        }
        else{
            return (int)(Math.round(((1-g)*data[i]+g*data[i+1])));
        }
    }

    /***
     * 将8位无符号数转变成8位有符号数
     * @param shortArray 8位无符号数
     * @return 8位有符号数
     */
    public static byte[] Short2Byte(short[] shortArray){
        int length = shortArray.length;
        byte[] byteArray = new byte[length];
        for(int i=0;i<length;i++){
            byteArray[i] = (shortArray[i]>127)?(byte)(shortArray[i]-256):(byte)shortArray[i];
        }
        return byteArray;
    }

    /***
     * 将8位有符号数转变成8位无符号数
     * @param byteArray 8位有符号数
     * @return 8位无符号数
     */
    public static short[] Byte2Short(byte[] byteArray){
        int length = byteArray.length;
        short[] shortArray = new short[length];
        for(int i=0;i<length;i++){
            shortArray[i] = (byteArray[i]<0)?(short)(byteArray[i]+256):(short)byteArray[i];
        }
        return shortArray;
    }

    public static String exec(String command) {
        Runtime runtime = Runtime.getRuntime();
        StringBuilder res = new StringBuilder();
        Process process = null;
        BufferedReader in = null;
        if (runtime == null) {
            throw new RuntimeException("Create runtime error!");
        }
        try {
//            log.info("exec[" + command + "]");
//            process = runtime.exec(new String[]{"cmd", "/c", command});
            process = runtime.exec(new String[]{"/bin/sh", "-c", command});
            in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                res.append(line).append("\n");
            }
            int exitVal = process.waitFor();
            if (exitVal != 0) {
                return "failed";
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            // kill process and free io stream.
            if (process != null) {
                process.destroy();
            }
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return res.toString();
    }
}
