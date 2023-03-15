package org.zju.zkw.rasterprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zju.zkw.subimg.SubImage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ShellCreate {
    private static final Logger logger = LoggerFactory.getLogger(SubImage.class);
    public static void createShell(String path,String[] strings) throws IOException {
        if(strings==null){
            logger.error("strings is null");
            return;
        }
        File sh = new File(path);
        if(!sh.createNewFile()){
            logger.error("create shell failed");
        }
        if(!sh.setExecutable(true)){
            logger.error("execute failed");
        }
        FileWriter fw = new FileWriter(sh);
        BufferedWriter bf = new BufferedWriter(fw);
        bf.write("#! /bin/bash");
        bf.newLine();
        for(int i=0;i<strings.length;i++){
            bf.write(strings[i]);
            if(i<strings.length-1){
                bf.newLine();
            }
        }
        bf.flush();
        bf.close();
    }

}
