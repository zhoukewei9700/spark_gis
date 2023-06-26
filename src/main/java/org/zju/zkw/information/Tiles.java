package org.zju.zkw.information;

import lombok.Getter;
import lombok.Setter;
import org.bson.types.Binary;
import java.io.Serializable;
@Setter
@Getter
public class Tiles implements Serializable {
    public int z;
    public int x;
    public int y;
    public Binary binaryData;
    public Tiles(int z, int x, int y) {
        this.z = z;
        this.x = x;
        this.y = y;
    }

    public Tiles(int z, int x, int y, Binary binaryData) {
        this.z = z;
        this.x = x;
        this.y = y;
        this.binaryData = binaryData;
    }
}
