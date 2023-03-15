package org.zju.zkw.information;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.io.Serializable;
import static org.zju.zkw.Constant.EXTENT_SEPORATOR;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Extent implements Serializable {
    double minX;
    double maxX;
    double minY;
    double maxY;

    public Extent(String s) {
        String[] extenets = s.split(EXTENT_SEPORATOR);
        this.minX = Double.parseDouble(extenets[0]);
        this.maxX = Double.parseDouble(extenets[1]);
        this.minY = Double.parseDouble(extenets[2]);
        this.maxY = Double.parseDouble(extenets[3]);
    }
    public String toWkt() {
        return "POLYGON((" + this.minX + " " + this.minY + "," + this.minX + " " + this.maxY + ","
                + this.maxX + " " + this.maxY + "," + this.maxX + " " + this.minY + "))";
    }
    public boolean intersect(Extent extent) {
        return intersect(extent.getMinX(), extent.getMaxX(), extent.getMinY(), extent.getMaxY());
    }

    public boolean intersect(double minx, double maxx, double miny, double maxy) {
        return !(this.maxX < minx || this.maxY < miny || this.minX > maxx || this.minY > maxy);
    }

}
