package config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigLoader {
    private static Properties properties;
    private static final String projectConfigPath = "src/main/java/config/config.properties";

    static {
        properties = new Properties();
        try {
            FileInputStream fis = new FileInputStream(projectConfigPath);
            properties.load(fis);
            fis.close();

            // Resolve ${user.dir} placeholder in properties
            String userDir = System.getProperty("user.dir");
            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                value = value.replace("${user.dir}", userDir);
                properties.setProperty(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception appropriately (e.g., logging, default values, etc.)
            throw new RuntimeException("Error loading config.properties: " + e.getMessage());
        }
    }


    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static int getIntProperty(String key) {
        String value = properties.getProperty(key);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new RuntimeException("Error parsing int value for key '" + key + "': " + e.getMessage());
        }
    }
}
