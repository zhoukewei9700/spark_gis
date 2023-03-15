package org.zju.zkw.information;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.gdal.gdal.GCP;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ProjectInfo implements Serializable {

    private String projWKT;		//投影字符串
    private double[] arrGeoTransform;	//仿射变换系数
    int iGCPNum;				//GCP个数
    List<GCP> GCPs;		//GCP数据

}

