package com.jushan.framework.ws;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * WebSocket + STOMP 基础配置。
 * <p>
 * 为平台内部管理后台和岗亭端提供实时推送能力。
 * <p>
 * <strong>架构说明</strong>：
 * <ul>
 *   <li>客户端通过 STOMP over WebSocket 连接</li>
 *   <li>使用内置 Simple Broker（pub/sub + user queue）</li>
 *   <li>生产环境建议启用外部 STOMP broker（如 RabbitMQ STOMP plugin），
 *       由 Nginx 代理 WebSocket 连接</li>
 *   <li>后续 T11 Docker Compose + Nginx 配置 WebSocket 代理</li>
 * </ul>
 * <p>
 * <strong>目标前缀约定</strong>：
 * <ul>
 *   <li>{@code /app/**} — 客户端 → 服务端（@MessageMapping 处理）</li>
 *   <li>{@code /topic/**} — 服务端 → 客户端广播（按停车场/租户订阅）</li>
 *   <li>{@code /user/**} — 服务端 → 客户端私信（convertAndSendToUser）</li>
 * </ul>
 * <p>
 * <strong>安全</strong>：握手鉴权和通道级数据范围校验分别由
 * {@link WsAuthHandshakeInterceptor} 和 {@link WsChannelAuthInterceptor} 实现。
 * <p>
 * <strong>P0 红线</strong>：WebSocket 不能直连 Device Access；
 * 推送失败不得阻断入场等核心业务事务。
 *
 * @author Jushan Platform
 * @since 1.0.0
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final WsChannelAuthInterceptor channelAuthInterceptor;

    public WebSocketConfig(WsChannelAuthInterceptor channelAuthInterceptor) {
        this.channelAuthInterceptor = channelAuthInterceptor;
    }

    /** STOMP 端点路径 */
    static final String STOMP_ENDPOINT = "/ws";

    /**
     * 允许跨域来源（仅本地开发默认值）。
     * 生产环境通过 {@code app.ws.allowed-origins} 覆盖（逗号分隔），避免把服务器地址写入源码。
     */
    @Value("${app.ws.allowed-origins:http://localhost:5173,http://localhost:5174,http://localhost:8080,http://localhost:8088,http://localhost:3000,http://localhost:3001}")
    private String[] allowedOrigins;

    /** 应用目标前缀（客户端 → 服务端） */
    static final String APP_DESTINATION_PREFIX = "/app";

    /** 用户目标前缀（服务端 → 特定用户） */
    static final String USER_DESTINATION_PREFIX = "/user";

    // ==================== STOMP 端点 ====================

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // 原生 WebSocket 端点（@stomp/stompjs brokerURL 直连）
        // setAllowedOriginPatterns 支持通配符，生产环境允许任意来源
        registry.addEndpoint(STOMP_ENDPOINT)
                .setAllowedOriginPatterns("*")
                .addInterceptors(new WsAuthHandshakeInterceptor());
        // SockJS 降级端点（对 WebSocket 不可用的浏览器）
        registry.addEndpoint(STOMP_ENDPOINT)
                .setAllowedOriginPatterns("*")
                .addInterceptors(new WsAuthHandshakeInterceptor())
                .withSockJS();
    }

    // ==================== 消息代理 ====================

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // 内置 Simple Broker：服务端 → 客户端广播/单播
        registry.enableSimpleBroker("/topic", "/queue");
        // /user 前缀转换为用户专属队列（执行 /user/{username}/** 映射）
        registry.setUserDestinationPrefix(USER_DESTINATION_PREFIX);
        // 客户端发送消息的目标前缀（映射到 @MessageMapping）
        registry.setApplicationDestinationPrefixes(APP_DESTINATION_PREFIX);
    }

    // ==================== 通道拦截器 ====================

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(channelAuthInterceptor);
    }
}
