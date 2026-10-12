package com.smartparking.deviceaccess.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Device Access 生产环境关键配置校验器。
 * <p>
 * 仅在 {@code prod} Profile 激活时运行，逐项检查必需配置是否设置。
 * 缺失任一关键安全配置时抛出 {@link IllegalStateException}，使应用启动失败关闭。
 * <p>
 * <strong>安全原则</strong>：宁可启动失败，不可在缺少关键安全配置的状态下运行。
 *
 * @author Jushan Platform
 * @since 1.0.0
 */
@Component
@Profile("prod")
public class ProdConfigValidator implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ProdConfigValidator.class);

    /** 开发期占位 API Key，禁止带入生产 */
    private static final String DEV_DEFAULT_API_KEY = "test-api-key-001";

    private final Environment env;

    public ProdConfigValidator(Environment env) {
        this.env = env;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("执行 Device Access 生产环境关键配置校验...");

        List<String> missing = new ArrayList<>();

        // ---- 数据库 ----
        checkRequired("spring.datasource.url", "数据库连接", missing);
        checkRequired("spring.datasource.username", "数据库用户名", missing);
        checkRequired("spring.datasource.password", "数据库密码", missing);

        // ---- MQTT / EMQX ----
        checkRequired("device-access.mqtt.broker-url", "MQTT Broker 地址", missing);
        checkRequired("device-access.mqtt.password", "MQTT 密码", missing);

        // ---- 对外 API Key 认证（禁止使用开发占位值） ----
        String apiKey = env.getProperty("device-access.auth.keys[0]");
        if (apiKey == null || apiKey.isBlank()) {
            missing.add("API Key 认证密钥 (device-access.auth.keys[0] / API_KEY_1)");
        } else if (DEV_DEFAULT_API_KEY.equals(apiKey)) {
            missing.add("API Key 认证密钥不能使用开发默认值 (device-access.auth.keys[0])");
        }

        // ---- Webhook 签名密钥 ----
        checkRequired("device-access.webhook.secret-key", "Webhook 签名密钥", missing);

        // ---- 抓拍图片公网访问地址 ----
        checkRequired("device-access.image.public-base-url", "图片公网访问地址", missing);

        if (!missing.isEmpty()) {
            String msg = "Device Access 生产环境关键配置缺失，应用拒绝启动。缺失项: " + String.join(", ", missing);
            log.error(msg);
            throw new IllegalStateException(msg);
        }

        log.info("Device Access 生产环境关键配置校验通过");
    }

    private void checkRequired(String property, String description, List<String> missing) {
        String value = env.getProperty(property);
        if (value == null || value.isBlank()) {
            missing.add(description + " (" + property + ")");
        }
    }
}
