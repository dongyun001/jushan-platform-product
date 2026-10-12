package com.jushan.boot.config;

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
 * 生产环境关键配置校验器。
 * <p>
 * 仅在 {@code prod} Profile 激活时运行，逐项检查必需环境变量是否设置。
 * 缺失任一关键配置时抛出 {@link IllegalStateException}，使应用启动失败关闭。
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

    private final Environment env;

    public ProdConfigValidator(Environment env) {
        this.env = env;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("执行生产环境关键配置校验...");

        // 当前 T06 阶段数据库尚未启用，仅校验通用安全配置。
        // 每个阶段（T07/T12/T42）应在对应模块中补充校验项。

        List<String> missing = new ArrayList<>();

        // 通用安全配置
        checkRequired("server.port", "服务端口", missing);

        // ---- 数据库 ----
        checkRequired("spring.datasource.url", "数据库连接", missing);
        checkRequired("spring.datasource.username", "数据库用户名", missing);
        checkRequired("spring.datasource.password", "数据库密码", missing);

        // ---- Redis ----
        checkRequired("spring.data.redis.host", "Redis 地址", missing);
        // Redis 密码：prod 下不得为空，否则 Redis 无鉴权暴露
        checkRequired("spring.data.redis.password", "Redis 密码", missing);

        // ---- Sa-Token ----
        checkRequired("sa-token.token-name", "Token 名称", missing);

        // ---- 安全：管理员初始密码（FIX-04） ----
        String initialAdminPassword = env.getProperty("app.initial-admin-password");
        if (initialAdminPassword == null || initialAdminPassword.isBlank()) {
            missing.add("管理员初始密码 (app.initial-admin-password)");
        }

        // ---- Device Access（FIX-14） ----
        String daBaseUrl = env.getProperty("jushan.device-access.base-url");
        if (daBaseUrl == null || daBaseUrl.isBlank()) {
            missing.add("Device Access 地址 (jushan.device-access.base-url)");
        } else if (daBaseUrl.equals("http://localhost:8081")) {
            missing.add("Device Access 地址不能使用开发默认值 (jushan.device-access.base-url)");
        }
        checkRequiredRange("jushan.device-access.connect-timeout", "Device Access 连接超时",
                1000, 30000, missing);
        checkRequiredRange("jushan.device-access.read-timeout", "Device Access 读取超时",
                1000, 30000, missing);

        // ---- Device Access 内部通信鉴权（不得使用开发默认值） ----
        checkRequiredNotDevDefault("jushan.device-access.api-key", "Device Access API Key",
                "test-api-key-001", missing);
        checkRequiredNotDevDefault("jushan.device-access.whitelist-sync-api-key", "白名单同步 API Key",
                "local-whitelist-sync-key-dev", missing);

        // ---- 以下校验在对应模块启用后取消注释 ----
        // T42: checkRequired("wx.pay.mch-id", "微信支付商户号", missing);

        if (!missing.isEmpty()) {
            String msg = "生产环境关键配置缺失，应用拒绝启动。缺失项: " + String.join(", ", missing);
            log.error(msg);
            throw new IllegalStateException(msg);
        }

        log.info("生产环境关键配置校验通过");
    }

    private void checkRequired(String property, String description, List<String> missing) {
        String value = env.getProperty(property);
        if (value == null || value.isBlank()) {
            missing.add(description + " (" + property + ")");
        }
    }

    /**
     * 校验某个配置项非空，且不等于已知的开发默认值。
     * 用于防止开发期占位值被带到生产环境。
     */
    private void checkRequiredNotDevDefault(String property, String description,
                                             String devDefault, List<String> missing) {
        String value = env.getProperty(property);
        if (value == null || value.isBlank()) {
            missing.add(description + " (" + property + ")");
        } else if (value.equals(devDefault)) {
            missing.add(description + " 不能使用开发默认值 (" + property + ")");
        }
    }

    private void checkRequiredRange(String property, String description,
                                     int min, int max, List<String> missing) {
        String value = env.getProperty(property);
        if (value == null || value.isBlank()) {
            missing.add(description + " (" + property + ")");
            return;
        }
        try {
            int intValue = Integer.parseInt(value.trim());
            if (intValue < min || intValue > max) {
                missing.add(description + " 超出范围 [" + min + ", " + max + "] (" + property + "=" + intValue + ")");
            }
        } catch (NumberFormatException e) {
            missing.add(description + " 格式无效 (" + property + "=" + value + ")");
        }
    }
}
