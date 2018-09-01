package com.hitbd.proj;

import com.hitbd.proj.logic.hbase.HbaseUpload;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: trafficBD [Action] [Parameter]");
            System.out.println("Actions:");
            System.out.println("ImportAlarm [FileName/Folder] [HBaseConfFile] import alarm file to hbase");
        }
        loadSettings();
        if (args[0].equals("ImportAlarm")) {
            HbaseUpload.main(args);
        }
    }

    public static void loadSettings() {
        File settings;
        try {
            settings = new File ("conf/settings");
            if (settings.exists()){
                Scanner scanner = new Scanner(settings);
                while (scanner.hasNext()) {
                    String[] conf = scanner.nextLine().split("=");
                    String key = conf[0].trim();
                    String value = conf[1].trim();
                    switch (key) {
                        case "igniteHostAddress":
                            Settings.igniteHostAddress = value;
                            break;
                        case "logDir":
                            Settings.logDir = value;
                            break;
                        default:
                            System.out.println("Cannot resolve attribute: " + key);
                    }
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }catch (IndexOutOfBoundsException e2) {
            System.out.println("config file should have format like key:value");
            e2.printStackTrace();
        }
    }
}
