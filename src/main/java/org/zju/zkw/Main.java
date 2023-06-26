package org.zju.zkw;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zju.zkw.subimg.SubImageV7;

import java.io.IOException;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(SubImageV7.class);
    public static void main(String @NotNull[] args) throws IOException, InterruptedException {
        String tifPath = args[0];
        String pngPath = args[1];
        String zRange = args[2];
        String shellName = "/root/zkw/start-subimage.sh";
        String[] command = new String[2];
        command[0] = "spark-submit --class org.zju.zkw.subimg.SubImageV2 --master yarn --deploy-mode cluster --driver-memory 2g --num-executors 32 --executor-memory 2g --executor-cores 2 "+tifPath+" "+pngPath+" "+zRange;
        command[1] = "mb-util --scheme=tms /root/zkw/OutPng /root/zkw/mbtiles/subOut.mbtiles";
        //ShellCreate.createShell(shellName,command);
        ProcessBuilder pb = new ProcessBuilder("/root/zkw/start-subimage.sh");
        int runningStatus = 0;
        String s = null;
        try {
            Process p = pb.start();
            try {
                runningStatus = p.waitFor();
            } catch (InterruptedException e) {
                throw e;
            }

        } catch (IOException | InterruptedException e) {
            throw  e;
        }
        if (runningStatus != 0) {
            logger.error("running failed");
        }

    }
}
