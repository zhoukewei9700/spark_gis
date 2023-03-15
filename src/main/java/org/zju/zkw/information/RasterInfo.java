package org.zju.zkw.information;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)

public class RasterInfo implements Serializable{
    private int rows;  //行数
    private int cols;  //列数
    private int bandNum; //条带数
    private Extent extent;
    private ProjectInfo projectInfo;
    private int gdalType;

    public RasterInfo deepCopy() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (RasterInfo) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("copy error");
        }
    }
}
