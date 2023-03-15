package org.zju.zkw.information;
import lombok.Getter;

public enum ImageType {

    PNG("png"), TIF("tif");

    @Getter
    private final String name;

    ImageType(String name) {
        this.name = name;
    }
}
