package org.zju.zkw.rasterprocess;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Slf4j
public class ShellUtil {



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
