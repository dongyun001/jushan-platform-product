package com.smartparking.deviceaccess.adapter.zhenshi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartparking.deviceaccess.adapter.support.DeviceHeartbeatRecorder;
import com.smartparking.deviceaccess.adapter.zhenshi.model.*;
import com.smartparking.deviceaccess.common.dto.mqtt.MqttMessage;
import com.smartparking.deviceaccess.common.dto.mqtt.MqttRequestPayload;
import com.smartparking.deviceaccess.common.enums.DisplayDirection;
import com.smartparking.deviceaccess.common.event.PlateRecognizedData;
import com.smartparking.deviceaccess.common.event.PlateRecognizedListener;
import com.smartparking.deviceaccess.mqtt.MqttGateway;
import com.smartparking.deviceaccess.mqtt.MqttMessageListener;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 臻识 C5H 消息处理器。
 * <p>
 * 职责：
 * <ul>
 *   <li>注册为 MQTT 全局监听器</li>
 *   <li>按 message.name 路由到对应的解析方法</li>
 *   <li>将 payload 解析为统一的 {@link ZhenshiEvent} 子类</li>
 *   <li>构建并发送下行命令（校时）</li>
 * </ul>
 * <p>
 * 当前 v0.1 仅接入臻识 C5H 摄像头，不接道闸。
 * 事件仅打印日志，后续接入 EventBus 后从这里输出统一事件。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ZhenshiMessageHandler implements MqttMessageListener {

    private final MqttGateway mqttGateway;
    private final ObjectMapper objectMapper;
    private final DeviceHeartbeatRecorder heartbeatRecorder;

    /** 车牌识别事件回调（可选注入，由 api 模块 PlateRecognizedEventDispatcher 实现） */
    @Autowired(required = false)
    private PlateRecognizedListener plateListener;

    /** 图片公网访问基础 URL（如 https://<域名>/images），用于构造 FTP 上传图片的对外地址 */
    @org.springframework.beans.factory.annotation.Value("${device-access.image.public-base-url:http://localhost:8082/images}")
    private String imagePublicBaseUrl;

    /** 臻识相机 FTP 上传子目录（相机配置中填写的上传路径），默认 parking */
    @org.springframework.beans.factory.annotation.Value("${device-access.image.zhenshi-ftp-subdir:parking}")
    private String zhenshiFtpSubdir;

    // ──────────────────── 消息名常量 ────────────────────

    private static final String NAME_KEEP_ALIVE = "keep_alive";
    private static final String NAME_IVS_RESULT = "ivs_result";
    private static final String NAME_QUICK_IVS_RESULT = "quick_ivs_result";
    private static final String NAME_SNAPSHOT = "snapshot";

    // ──────────────────── Topic 模板 ────────────────────

    private static final String TOPIC_PREFIX = "device/";
    private static final String TOPIC_SET_TIME = "device/%s/message/down/set_time";
    private static final String TOPIC_SERIAL_DATA = "device/%s/message/down/serial_data";
    private static final String TOPIC_GATE_DIRECT_OPEN = "device/%s/message/down/gate_direct_open";
    private static final String TOPIC_SNAPSHOT = "device/%s/message/down/snapshot";
    private static final String TOPIC_UP_SNAPSHOT = "/message/up/snapshot";

    // ──────────────────── 最近车牌识别记录（用于日志上下文） ────────────────────

    private final ConcurrentHashMap<String, String> lastPlateMap = new ConcurrentHashMap<>();

    /**
     * 等待抓拍结果的 Future，key = deviceSn（小写）。
     * <p>
     * 臻识抓拍为两段式：下行 snapshot 命令仅回执确认，
     * 真正的抓图结果由设备通过 {@code device/{sn}/message/up/snapshot} 异步推送。
     */
    private final ConcurrentHashMap<String, CompletableFuture<MqttMessage>> pendingSnapshots = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        mqttGateway.registerListener(this);
        log.info("ZhenshiMessageHandler registered as MQTT listener");
    }

    // ──────────────────── 消息入口 ────────────────────

    @Override
    public void onMessage(String topic, MqttMessage message) {
        // 非臻识命名空间 Topic 静默忽略（如信路通 upload/...）
        if (topic == null || !topic.startsWith(TOPIC_PREFIX)) {
            return;
        }

        String name = message.getName();
        if (name == null) {
            log.warn("Received MQTT message without 'name' field. Topic: {}", topic);
            return;
        }

        try {
            switch (name) {
                case NAME_KEEP_ALIVE -> handleKeepAlive(message);
                case NAME_IVS_RESULT, NAME_QUICK_IVS_RESULT -> handlePlateRecognition(message, name);
                case NAME_SNAPSHOT -> handleSnapshotResult(topic, message);
                default -> log.debug("Unhandled message type: {}, Topic: {}", name, topic);
            }
        } catch (Exception e) {
            log.error("Error handling MQTT message. Name: {}, Sn: {}", name, message.getSn(), e);
        }
    }

    // ──────────────────── 消息处理 ────────────────────

    /**
     * 处理心跳 —— 更新缓存 + DB 持久化，设备在线判定依据。
     */
    @SuppressWarnings("unchecked")
    private void handleKeepAlive(MqttMessage message) {
        String sn = message.getSn();
        Long deviceTimestamp = null;

        Object payload = message.getPayload();
        if (payload instanceof Map<?, ?> payloadMap) {
            Object body = payloadMap.get("body");
            if (body instanceof Map<?, ?> bodyMap) {
                Object ts = bodyMap.get("timestamp");
                if (ts instanceof Number num) {
                    deviceTimestamp = num.longValue();
                }
            }
        }

        heartbeatRecorder.recordHeartbeat(sn);

        HeartbeatEvent event = new HeartbeatEvent(sn, message.getTimestamp(), deviceTimestamp);
        log.debug("Heartbeat received: sn={}", sn);

        // TODO v0.3: publish event to EventBus
    }

    /**
     * 处理车牌识别结果。
     */
    @SuppressWarnings("unchecked")
    private void handlePlateRecognition(MqttMessage message, String name) {
        Object payload = message.getPayload();
        if (!(payload instanceof Map<?, ?> payloadMap)) {
            log.warn("Unexpected payload type for {}: {}", name, payload.getClass());
            return;
        }

        Map<String, Object> alarmInfo = (Map<String, Object>) payloadMap.get("AlarmInfoPlate");
        if (alarmInfo == null) {
            log.warn("No AlarmInfoPlate in {} payload", name);
            return;
        }

        Map<String, Object> result = (Map<String, Object>) alarmInfo.get("result");
        Map<String, Object> plateResult = result != null
                ? (Map<String, Object>) result.get("PlateResult") : null;

        // 解析车牌号（臻识用 Base64 编码）
        String licenseEncoded = plateResult != null
                ? (String) plateResult.get("license") : null;
        String license = decodeBase64(licenseEncoded);

        // 解析 plates 数组取第一张车牌
        List<Map<String, Object>> plates = plateResult != null
                ? (List<Map<String, Object>>) plateResult.get("plates") : null;
        Map<String, Object> firstPlate = (plates != null && !plates.isEmpty())
                ? plates.get(0) : null;

        // 图片地址同样为 Base64 编码（解码后为可访问 URL，如 OSS 签名地址），
        // 与 license/deviceName 一样需要解码，否则前端无法渲染
        String rawImagePath = plateResult != null ? (String) plateResult.get("imagePath") : null;
        String imagePath = (rawImagePath != null && !rawImagePath.isBlank())
                ? decodeBase64(rawImagePath) : null;
        String rawPlateImagePath = firstPlate != null ? (String) firstPlate.get("image_path") : null;
        String plateImagePath = (rawPlateImagePath != null && !rawPlateImagePath.isBlank())
                ? decodeBase64(rawPlateImagePath) : null;

        // 臻识 C5 的 imagePath/plateImagePath 为相机本地文件标识（非 HTTP URL）时，
        // 按 FTP 上传路径构造对外可访问的 HTTP URL（相机已配置 FTP 上传到 nginx 静态目录）
        String ipAddr = (String) alarmInfo.get("ipaddr");
        long eventTs = message.getTimestamp() != null ? message.getTimestamp() : 0L;
        if (imagePath == null || !imagePath.startsWith("http")) {
            imagePath = buildFtpImageUrl(ipAddr, license, eventTs, false);
        }
        if (plateImagePath == null || !plateImagePath.startsWith("http")) {
            plateImagePath = buildFtpImageUrl(ipAddr, license, eventTs, true);
        }

        PlateRecognizedEvent event = PlateRecognizedEvent.builder()
                .sn(message.getSn())
                .eventTimestamp(message.getTimestamp())
                .deviceName(decodeBase64((String) alarmInfo.get("deviceName")))
                .ipAddr((String) alarmInfo.get("ipaddr"))
                .channel((Integer) alarmInfo.get("channel"))
                .ruleId((Integer) alarmInfo.get("rule_id"))
                .license(license)
                .plateColor(firstPlate != null ? (Integer) firstPlate.get("color") : null)
                .plateType(firstPlate != null ? (Integer) firstPlate.get("type") : null)
                .confidence(plateResult != null ? (Integer) plateResult.get("confidence") : null)
                .direction(plateResult != null ? (Integer) plateResult.get("direction") : null)
                .carColor(plateResult != null ? (Integer) plateResult.get("carColor") : null)
                .carBrand(extractCarBrand(plateResult))
                .isDanger(firstPlate != null ? (Integer) firstPlate.get("is_danger") : null)
                .triggerType(plateResult != null ? (Integer) plateResult.get("triggerType") : null)
                .isOffline(plateResult != null ? (Integer) plateResult.get("isoffline") : null)
                .imagePath(imagePath)
                .plateImagePath(plateImagePath)
                .startTime(plateResult != null ? toLong(plateResult.get("start_time")) : null)
                .build();

        log.info("Plate recognized: sn={}, license={}, confidence={}",
                event.getSn(), event.getLicense(), event.getConfidence());

        // 记录最近车牌，用于命令日志上下文
        if (event.getLicense() != null && !event.getLicense().isBlank()) {
            lastPlateMap.put(event.getSn(), event.getLicense());
        }

        // v0.4: 通过回调接口推送车牌识别事件到业务侧
        // quick_ivs_result 是快速预览（无图片），约 100ms 后会有带图片的 ivs_result 到达；
        // 两条都分发时，平台 BR-08 去重规则会把后到的那条（恰是带图片的）判为重复丢弃，
        // 导致通行记录永远无图。因此 quick 事件只记录日志，不下发业务侧。
        if (NAME_QUICK_IVS_RESULT.equals(name)) {
            log.debug("quick_ivs_result 仅预览不下发（等待 ivs_result 携带图片）: sn={}, license={}",
                    event.getSn(), event.getLicense());
            return;
        }
        if (plateListener != null) {
            try {
                PlateRecognizedData data = new PlateRecognizedData(
                        event.getSn(),
                        event.getLicense(),
                        event.getConfidence(),
                        event.getDirection(),
                        event.getPlateColor(),
                        event.getImagePath(),
                        event.getPlateImagePath(),   // 臻识通常无车牌特写图字段（为空时为 null）
                        event.getStartTime()
                );
                plateListener.onPlateRecognized(data);
            } catch (Exception e) {
                log.error("Failed to notify plate listener: sn={}, license={}",
                        event.getSn(), event.getLicense(), e);
            }
        }
    }

    // ──────────────────── 下行命令 ────────────────────

    /**
     * 下发校时命令，异步返回设备回复。
     *
     * @param deviceSn       设备序列号
     * @param timeoutSeconds 超时时间（秒）
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> sendSyncTime(String deviceSn, long timeoutSeconds) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        Instant instant = Instant.now();
        ZoneId zone = ZoneId.of("Asia/Shanghai");
        DateTimeFormatter yearFmt = DateTimeFormatter.ofPattern("yyyy").withZone(zone);
        DateTimeFormatter twoDigitFmt = DateTimeFormatter.ofPattern("MM|dd|HH|mm|ss").withZone(zone);
        String[] parts = twoDigitFmt.format(instant).split("\\|");

        SetTimeBody body = SetTimeBody.builder()
                .year(yearFmt.format(instant))
                .month(parts[0])
                .day(parts[1])
                .hour(parts[2])
                .min(parts[3])
                .sec(parts[4])
                .build();

        MqttRequestPayload requestPayload = MqttRequestPayload.builder()
                .type("set_time")
                .body(body)
                .build();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("set_time")
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format(TOPIC_SET_TIME, deviceSn);
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS);
    }

    // ═══════════════════════════════════════════
    // 道闸控制（v0.4 新增）
    // ═══════════════════════════════════════════

    /**
     * 下发开闸命令。
     * <p>
     * 使用 gpio_out: IO0, value=2（先通后断脉冲）, delay=1500ms。
     * 臻识 C5H 固件 bv=16771 不支持 gate_direct_open。
     */
    public CompletableFuture<MqttMessage> sendOpenGate(String deviceSn, long timeoutSeconds) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        Map<String, Object> body = Map.of("io", 0, "value", 2, "delay", 1500);

        MqttRequestPayload requestPayload = MqttRequestPayload.builder()
                .type("gpio_out")
                .body(body)
                .build();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("gpio_out")
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format("device/%s/message/down/gpio_out", deviceSn);
        log.info("[Gate] SEND open  deviceSn={}  io=0 value=2 delay=1500  topic={}", deviceSn, topic);
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * 下发关闸命令。
     * <p>
     * 使用 gpio_out: IO1, value=2（先通后断脉冲）, delay=3000ms。
     */
    public CompletableFuture<MqttMessage> sendCloseGate(String deviceSn, long timeoutSeconds) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        Map<String, Object> body = Map.of("io", 1, "value", 2, "delay", 3000);

        MqttRequestPayload requestPayload = MqttRequestPayload.builder()
                .type("gpio_out")
                .body(body)
                .build();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("gpio_out")
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format("device/%s/message/down/gpio_out", deviceSn);
        log.info("[Gate] SEND close  deviceSn={}  io=1 value=2 delay=3000  topic={}", deviceSn, topic);
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * 下发直接开闸命令（不指定 IO 端口）。
     * <p>
     * 使用协议 7.19 gate_direct_open：payload 为空，由摄像头自行选择 IO 端口执行开闸。
     * 主要用于解锁道闸后重置内部道闸状态机，恢复 gpio_out 命令的正常响应。
     * <p>
     * 注意：需要固件版本 bv >= 对应 v1.1.14 协议版本。
     */
    public CompletableFuture<MqttMessage> sendGateDirectOpen(String deviceSn, long timeoutSeconds) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("gate_direct_open")
                .version("1.0")
                .timestamp(now)
                .payload(null)
                .build();

        String topic = String.format(TOPIC_GATE_DIRECT_OPEN, deviceSn);
        log.info("[Gate] SEND gate_direct_open  deviceSn={}  topic={}", deviceSn, topic);
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS);
    }

    // ──────────────────── 主动抓拍 ────────────────────

    /**
     * 下发抓拍命令并等待异步抓图结果。
     * <p>
     * 流程（协议 7.6/6.5）：
     * 1. 发布 {@code device/{sn}/message/down/snapshot}，回执仅确认设备受理（code=200）；
     * 2. 设备抓图完成后通过 {@code device/{sn}/message/up/snapshot} 推送结果，
     *    payload 含 state_code、image_content（Base64 背景图）等。
     * <p>
     * 为避免竞态（设备可能极快推送结果），先注册等待 Future 再下发命令。
     *
     * @param deviceSn       设备序列号
     * @param timeoutSeconds 整体超时（命令回执 + 抓图结果）
     * @return 抓图结果消息（payload 为 Map，含 state_code/image_content 等）
     */
    public CompletableFuture<MqttMessage> sendSnapshot(String deviceSn, long timeoutSeconds) {
        String key = deviceSn.toLowerCase();
        CompletableFuture<MqttMessage> resultFuture = new CompletableFuture<>();
        pendingSnapshots.put(key, resultFuture);
        resultFuture.orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .whenComplete((r, e) -> pendingSnapshots.remove(key));

        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        MqttRequestPayload requestPayload = MqttRequestPayload.builder()
                .type("snapshot")
                .body(Map.of())
                .build();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name(NAME_SNAPSHOT)
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format(TOPIC_SNAPSHOT, deviceSn);
        log.info("[Capture] SEND snapshot  deviceSn={}  topic={}", deviceSn, topic);

        // 剩余超时 = 总超时 - 回执耗时，简化处理：回执与结果共用同一总超时
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS)
                .thenCompose(ack -> {
                    if (ack.getCode() != null && ack.getCode() != 200) {
                        resultFuture.cancel(false);
                        throw new RuntimeException("snapshot command rejected by device, code=" + ack.getCode());
                    }
                    log.info("[Capture] ACK snapshot  deviceSn={}  code={}", deviceSn, ack.getCode());
                    return resultFuture;
                });
    }

    /**
     * 处理设备异步推送的抓图结果（up/snapshot），完成等待中的 Future。
     * <p>
     * 注意：下行命令的回执（down/snapshot/reply）消息 name 同样为 snapshot，
     * 但其 Future 由 MqttGateway 按消息 id 完成，这里只处理 up 推送。
     */
    private void handleSnapshotResult(String topic, MqttMessage message) {
        if (topic == null || !topic.contains(TOPIC_UP_SNAPSHOT)) {
            return;
        }
        String sn = message.getSn();
        if (sn == null) {
            log.warn("[Capture] snapshot result without sn, topic={}", topic);
            return;
        }
        CompletableFuture<MqttMessage> future = pendingSnapshots.remove(sn.toLowerCase());
        if (future != null) {
            log.info("[Capture] RECV snapshot result  deviceSn={}", sn);
            future.complete(message);
        } else {
            log.info("[Capture] snapshot result with no waiter (设备主动推送?)  deviceSn={}", sn);
        }
    }

    /**
     * [临时测试] 下发 gpio_out 命令，支持任意 io/value/delay 组合。
     */
    public CompletableFuture<MqttMessage> sendGpioTest(String deviceSn, int io, int value, int delay, long timeoutSeconds) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        Map<String, Object> body;
        if (delay > 0) {
            body = Map.of("io", io, "value", value, "delay", delay);
        } else {
            body = Map.of("io", io, "value", value);
        }

        MqttRequestPayload requestPayload = MqttRequestPayload.builder()
                .type("gpio_out")
                .body(body)
                .build();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("gpio_out")
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format("device/%s/message/down/gpio_out", deviceSn);
        log.info("[GateTest] SEND gpio_out  deviceSn={}  io={}  value={}  delay={}  topic={}",
                deviceSn, io, value, delay, topic);
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * 锁定道闸（持续吸合继电器）。
     * <p>
     * 使用臻识 set_io_lock_status 命令：
     * ioout=0 对应 IO1，ioout=1 对应 IO2
     * status=1 高电平锁定，status=2 低电平锁定。
     *
     * @param deviceSn       设备序列号
     * @param io             IO端口编号 (0=IO1, 1=IO2)
     * @param timeoutSeconds 超时时间（秒）
     */
    public CompletableFuture<MqttMessage> sendLockGate(String deviceSn, int io, long timeoutSeconds) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        // 臻识协议：ioout 0或1，status 1=高电平锁定 2=低电平锁定
        Map<String, Object> body = Map.of("ioout", io, "status", 1);

        MqttRequestPayload requestPayload = MqttRequestPayload.builder()
                .type("set_io_lock_status")
                .body(body)
                .build();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("set_io_lock_status")
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format("device/%s/message/down/set_io_lock_status", deviceSn);
        log.info("[Gate] SEND lock  deviceSn={}  ioout={}  status=1(high)  topic={}  (set_io_lock_status)", deviceSn, io, topic);
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * 解除道闸锁定（断开继电器）。
     * <p>
     * 使用臻识 set_io_lock_status 命令：
     * ioout=0 对应 IO1，ioout=1 对应 IO2
     * status=0 解锁。
     *
     * @param deviceSn       设备序列号
     * @param io             IO端口编号 (0=IO1, 1=IO2)
     * @param timeoutSeconds 超时时间（秒）
     */
    public CompletableFuture<MqttMessage> sendUnlockGate(String deviceSn, int io, long timeoutSeconds) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        // 臻识协议：ioout 0或1，status 0=解锁
        Map<String, Object> body = Map.of("ioout", io, "status", 0);

        MqttRequestPayload requestPayload = MqttRequestPayload.builder()
                .type("set_io_lock_status")
                .body(body)
                .build();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("set_io_lock_status")
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format("device/%s/message/down/set_io_lock_status", deviceSn);
        log.info("[Gate] SEND unlock  deviceSn={}  ioout={}  status=0(unlock)  topic={}  (set_io_lock_status)", deviceSn, io, topic);
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * 锁定道闸关闭（保持低电平锁定关闸方向）。
     * <p>
     * 使用臻识 set_io_lock_status 命令：
     * ioout=0 对应 IO0（开闸继电器），status=2 低电平锁定。
     * <p>
     * 与 {@link #sendLockGate} 的区别：锁定开闸是 status=1（高电平），锁定关闸是 status=2（低电平）。
     *
     * @param deviceSn       设备序列号
     * @param io             IO端口编号 (0=IO0)
     * @param timeoutSeconds 超时时间（秒）
     */
    public CompletableFuture<MqttMessage> sendLockCloseGate(String deviceSn, int io, long timeoutSeconds) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();

        // 臻识协议：ioout 0，status 2=低电平锁定关闸
        Map<String, Object> body = Map.of("ioout", io, "status", 2);

        MqttRequestPayload requestPayload = MqttRequestPayload.builder()
                .type("set_io_lock_status")
                .body(body)
                .build();

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("set_io_lock_status")
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format("device/%s/message/down/set_io_lock_status", deviceSn);
        log.info("[Gate] SEND lock-close  deviceSn={}  ioout={}  status=2(low)  topic={}  (set_io_lock_status)", deviceSn, io, topic);
        return mqttGateway.publishAndWait(topic, command, timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * 获取最近识别的车牌号（用于日志上下文）。
     *
     * @param deviceSn 设备序列号
     * @return 最近车牌号，无记录返回空字符串
     */
    public String getLastPlate(String deviceSn) {
        return lastPlateMap.getOrDefault(deviceSn, "");
    }

    // ──────────────────── RS485 串口透传 ────────────────────

    /**
     * 通过摄像头 RS485 端口向串口设备发送数据。
     * <p>
     * 构造臻识 serial_data MQTT 消息，摄像头收到后通过 RS485 转发原始字节。
     *
     * @param deviceSn      摄像头序列号
     * @param serialChannel RS485 通道号（默认 0）
     * @param rawData       原始字节（已构造好的外设协议帧）
     * @param interval      帧间隔毫秒（多帧发送时的间隔）
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> sendSerialData(String deviceSn, int serialChannel,
                                                          byte[] rawData, int interval) {
        String id = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();
        long startMs = System.currentTimeMillis();

        String base64Data = java.util.Base64.getEncoder().encodeToString(rawData);

        Map<String, Object> serialItem = Map.of(
                "serialChannel", serialChannel,
                "data", base64Data,
                "dataLen", rawData.length
        );

        Map<String, Object> body = Map.of(
                "interval", interval,
                "serialData", java.util.List.of(serialItem)
        );

        Map<String, Object> requestPayload = Map.of(
                "type", "serial_data",
                "body", body
        );

        MqttMessage command = MqttMessage.builder()
                .id(id)
                .sn(deviceSn)
                .name("serial_data")
                .version("1.0")
                .timestamp(now)
                .payload(requestPayload)
                .build();

        String topic = String.format(TOPIC_SERIAL_DATA, deviceSn);

        // ── 联调日志：发送前 ──
        log.info("[Display] SEND serial_data\n" +
                "  deviceSn={}\n" +
                "  messageId={}\n" +
                "  serialChannel={}\n" +
                "  interval={}ms\n" +
                "  topic={}\n" +
                "  payloadLength={}\n" +
                "  payloadHex=\n{}\n" +
                "{}",
                deviceSn, id, serialChannel, interval, topic,
                rawData.length, toHex(rawData),
                com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                        .formatFrameForDebug(rawData));

        CompletableFuture<MqttMessage> future = mqttGateway.publishAndWait(
                topic, command, 10, TimeUnit.SECONDS);

        // ── 联调日志：回执 / 超时 / 异常 ──
        future.whenComplete((reply, error) -> {
            long elapsed = System.currentTimeMillis() - startMs;
            if (error != null) {
                log.error("[Display] MQTT REPLY error\n" +
                        "  deviceSn={}\n" +
                        "  messageId={}\n" +
                        "  elapsedMs={}\n" +
                        "  errorType={}\n" +
                        "  errorMessage={}\n" +
                        "  topic={}",
                        deviceSn, id, elapsed,
                        error.getClass().getName(), error.getMessage(), topic);
                if (error instanceof java.util.concurrent.TimeoutException) {
                    log.error("[Display] COMMAND TIMEOUT\n" +
                            "  deviceSn={}\n" +
                            "  messageId={}\n" +
                            "  timeout=10s\n" +
                            "  payloadLength={}\n" +
                            "  payloadHex (first 80 chars)=\n{}",
                            deviceSn, id, rawData.length,
                            toHex(rawData).substring(0, Math.min(80, toHex(rawData).length())));
                }
            } else {
                String replyJson;
                try {
                    replyJson = objectMapper.writeValueAsString(reply);
                    if (replyJson.length() > 500) {
                        replyJson = replyJson.substring(0, 500) + "...(truncated)";
                    }
                } catch (Exception je) {
                    replyJson = "(serialization error: " + je.getMessage() + ")";
                }
                log.info("[Display] MQTT REPLY received\n" +
                        "  deviceSn={}\n" +
                        "  messageId={}\n" +
                        "  mqttCode={}\n" +
                        "  elapsedMs={}\n" +
                        "  rawReply={}",
                        deviceSn, id, reply.getCode(), elapsed, replyJson);
            }
        });

        return future;
    }

    /**
     * 设置音量。
     * <p>
     * 对应 OLM-M1D 0x0D 命令。
     *
     * @param deviceSn 摄像头序列号
     * @param percent  音量百分比（0~100）
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> setVolume(String deviceSn, int percent) {
        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildVolumeFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT,
                        percent);
        log.info("[Display] ACTION: SET_VOLUME  deviceSn={}  volume={}%  cmd=0x0D", deviceSn, percent);
        return sendSerialData(deviceSn, 0, frame, 50);
    }

    /**
     * 同步屏卡时间。
     * <p>
     * 对应 OLM-M1D 0x05 命令，使用服务器当前时间。
     *
     * @param deviceSn 摄像头序列号
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> syncDisplayTime(String deviceSn) {
        LocalDateTime now = LocalDateTime.now();
        int week = now.getDayOfWeek().getValue() % 7; // Monday=1->1, Sunday=7->0
        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildSyncTimeFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT,
                        now.getYear(), now.getMonthValue(), now.getDayOfMonth(),
                        now.getHour(), now.getMinute(), now.getSecond(), week);
        log.info("[Display] ACTION: SYNC_TIME  deviceSn={}  time={}  cmd=0x05",
                deviceSn, now.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        return sendSerialData(deviceSn, 0, frame, 50);
    }

    /**
     * 设置显示方向。
     * <p>
     * 对应 OLM-M1D 0x19 命令。
     *
     * @param deviceSn  摄像头序列号
     * @param direction 方向：0=正常，1=旋转180度
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> setDisplayDirection(String deviceSn, int direction) {
        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildDirectionFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT,
                        direction);
        log.info("[Display] ACTION: SET_DIRECTION  deviceSn={}  direction={}  cmd=0x19",
                deviceSn, direction == 1 ? "ROTATE_180" : "NORMAL");
        return sendSerialData(deviceSn, 0, frame, 50);
    }

    /**
     * 播放语音。
     * <p>
     * 对应 OLM-M1D 0x30 命令。
     *
     * @param deviceSn 摄像头序列号
     * @param voiceId  语音ID（0~341）
     * @param variable 变量替换文本（可选）
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> playVoice(String deviceSn, int voiceId, String variable) {
        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildPlayVoiceFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT, variable, voiceId);
        log.info("[Display] ACTION: PLAY_VOICE  deviceSn={}  voiceId={}  variable=\"{}\"  cmd=0x30",
                deviceSn, voiceId, variable != null ? variable : "");
        return sendSerialData(deviceSn, 0, frame, 50);
    }

    /**
     * 停止语音。
     * <p>
     * 对应 OLM-M1D 0x31 命令。
     *
     * @param deviceSn 摄像头序列号
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> stopVoice(String deviceSn) {
        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildStopVoiceFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT);
        log.info("[Display] ACTION: STOP_VOICE  deviceSn={}  cmd=0x31", deviceSn);
        return sendSerialData(deviceSn, 0, frame, 50);
    }

    /**
     * 增强版实时显示文字。
     * <p>
     * 支持字体、颜色、语音同步。
     * 对应 OLM-M1D 0x6F 命令（SF=0）。
     *
     * @param deviceSn       摄像头序列号
     * @param content        文本内容
     * @param direction      文本布局方向
     * @param font           字体类型
     * @param color          文字颜色 RGBA
     * @param voiceId        同步语音ID（可选）
     * @param voiceVariable  语音变量替换文本（可选）
     * @return Future，true 表示设备回复 ACK=0
     */
    public CompletableFuture<Boolean> displayTextEnhanced(String deviceSn, String content,
                                                           DisplayDirection direction,
                                                           com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.FontType font,
                                                           java.util.List<int[]> color,
                                                           Integer voiceId,
                                                           String voiceVariable) {
        List<String> lines = toLines(content, direction);

        // 构造语音参数
        byte[] voiceParams = null;
        if (voiceId != null) {
            java.nio.charset.Charset gbk = java.nio.charset.Charset.forName("GBK");
            byte[] varBytes = (voiceVariable != null && !voiceVariable.isEmpty())
                    ? voiceVariable.getBytes(gbk) : new byte[0];
            // VF(1) + VTL(1) + voiceId(1) + varBytes
            voiceParams = new byte[3 + varBytes.length];
            voiceParams[0] = 0x0A; // VF
            voiceParams[1] = (byte) (varBytes.length + 1); // VTL = varLen + 1 (voiceId byte)
            voiceParams[2] = (byte) (int) voiceId;
            if (varBytes.length > 0) {
                System.arraycopy(varBytes, 0, voiceParams, 3, varBytes.length);
            }
        }

        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildMultiLineFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT,
                        lines.size(),
                        lines,
                        font,
                        color,
                        voiceParams);
        log.info("[Display] ACTION: DISPLAY_TEXT_ENHANCED  deviceSn={}  direction={}  rows={}  font={}  voiceId={}  cmd=0x6F" +
                        "\n  content=\"{}\"",
                deviceSn, direction, lines.size(),
                font != null ? font.name() : "DEFAULT",
                voiceId != null ? voiceId : "NONE",
                content.replace("\n", "\\n"));
        return sendSerialData(deviceSn, 0, frame, 50)
                .thenApply(reply -> {
                    boolean ok = reply.getCode() != null && reply.getCode() == 200;
                    log.info("[Display] DISPLAY_TEXT_ENHANCED result: deviceSn={}  mqttCode={}  success={}",
                            deviceSn, reply.getCode(), ok);
                    return ok;
                });
    }

    // ═══════════════════════════════════════════
    // 外围设备控制（Peripheral Control）
    // ═══════════════════════════════════════════

    /**
     * 启用/关闭显示屏。
     * <p>
     * 通过亮度控制实现。enabled=true 时设置亮度为 80%，false 时为最低有效亮度。
     * 显示屏实际显示内容由控制卡自行管理，Device Access 不负责文本下发。
     *
     * @param deviceSn 摄像头序列号
     * @param enabled  true=启用，false=关闭
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> setDisplayEnabled(String deviceSn, boolean enabled) {
        int brightness = enabled ? 80
                : com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.BRIGHTNESS_OFF;
        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildBrightnessFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT,
                        brightness);
        log.info("[Display] ACTION: {}  deviceSn={}  brightness={}%  cmd=0x0C",
                enabled ? "ENABLE" : "DISABLE", deviceSn, brightness);
        return sendSerialData(deviceSn, 0, frame, 50);
    }

    /**
     * 设置显示屏模式（2行/4行）。
     * <p>
     * 通过 0x6F 单包多行显示命令配置显示卡的 zone 布局。
     * 帧中文本为空 —— Device Access 只配置布局，不管理显示内容。
     *
     * @param deviceSn 摄像头序列号
     * @param mode     显示模式（TWO_LINE / FOUR_LINE）
     * @return 设备回复消息的 Future
     */
    public CompletableFuture<MqttMessage> setDisplayMode(String deviceSn,
                                                          com.smartparking.deviceaccess.common.enums.DisplayMode mode) {
        int rows = (mode == com.smartparking.deviceaccess.common.enums.DisplayMode.TWO_LINE) ? 2 : 4;
        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildMultiLineFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT,
                        rows);
        log.info("[Display] ACTION: SET_MODE  deviceSn={}  mode={}  rows={}  cmd=0x6F",
                deviceSn, mode, rows);
        return sendSerialData(deviceSn, 0, frame, 50);
    }

    // ═══════════════════════════════════════════
    // 显示内容控制（v0.3 新增）
    // ═══════════════════════════════════════════

    /**
     * 实时显示文字。
     * <p>
     * 向控制卡临时区（RAM）下发文字内容，立即覆盖当前显示。
     * 内容掉电丢失，不影响控制卡存储区的内容。
     * <p>
     * 对应 OLM-M1D 0x6F 命令（SF=0）。
     *
     * @param deviceSn  摄像头序列号
     * @param content   文本内容，HORIZONTAL 时用 \n 分隔多行
     * @param direction 文本布局方向（当前仅支持 HORIZONTAL）
     * @return Future，true 表示设备回复 ACK=0
     */
    public CompletableFuture<Boolean> displayText(String deviceSn, String content,
                                                   DisplayDirection direction) {
        List<String> lines = toLines(content, direction);
        byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                .buildMultiLineFrame(
                        com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT,
                        lines.size(),
                        lines);
        log.info("[Display] ACTION: DISPLAY_TEXT  deviceSn={}  direction={}  rows={}  cmd=0x6F" +
                        "\n  content=\"{}\"",
                deviceSn, direction, lines.size(),
                content.replace("\n", "\\n"));
        return sendSerialData(deviceSn, 0, frame, 50)
                .thenApply(reply -> {
                    boolean ok = reply.getCode() != null && reply.getCode() == 200;
                    log.info("[Display] DISPLAY_TEXT result: deviceSn={}  mqttCode={}  success={}",
                            deviceSn, reply.getCode(), ok);
                    return ok;
                });
    }

    /**
     * 保存显示内容到控制卡。
     * <p>
     * 将文字逐行写入控制卡外部存储器（Flash），掉电不丢失。
     * 控制卡空闲时会自动循环显示已存储的持久化内容。
     * <p>
     * <b>低频操作。</b>每次调用会擦写控制卡 Flash，频繁调用会降低存储器寿命。
     * <p>
     * 对应 OLM-M1D 0x67 命令。
     *
     * @param deviceSn  摄像头序列号
     * @param content   文本内容，HORIZONTAL 时用 \n 分隔多行
     * @param direction 文本布局方向（当前仅支持 HORIZONTAL）
     * @return Future，true 表示所有行均发送成功且设备回复 ACK=0
     */
    public CompletableFuture<Boolean> saveDisplay(String deviceSn, String content,
                                                   DisplayDirection direction) {
        List<String> lines = toLines(content, direction);
        log.info("[Display] ACTION: SAVE_DISPLAY  deviceSn={}  direction={}  rows={}  cmd=0x67" +
                        "\n  content=\"{}\"",
                deviceSn, direction, lines.size(),
                content.replace("\n", "\\n"));

        return CompletableFuture.supplyAsync(() -> {
            boolean allOk = true;
            for (int i = 0; i < lines.size(); i++) {
                byte[] frame = com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol
                        .build0x67Frame(
                                com.smartparking.deviceaccess.adapter.support.display.OlmM1dProtocol.DA_DEFAULT,
                                i,
                                lines.get(i));
                log.info("[Display] SAVE row {}/{}: deviceSn={}  twid={}  text=\"{}\"",
                        i + 1, lines.size(), deviceSn, i, lines.get(i));
                try {
                    var reply = sendSerialData(deviceSn, 0, frame, 50)
                            .get(10, TimeUnit.SECONDS);
                    boolean ok = reply.getCode() != null && reply.getCode() == 200;
                    if (!ok) {
                        log.warn("[Display] SAVE row {}/{} FAILED: deviceSn={}  mqttCode={}",
                                i + 1, lines.size(), deviceSn, reply.getCode());
                        allOk = false;
                    } else {
                        log.info("[Display] SAVE row {}/{} OK: deviceSn={}  mqttCode=200",
                                i + 1, lines.size(), deviceSn);
                    }
                    // 行间短暂间隔，避免控制卡串口缓冲区溢出
                    if (i < lines.size() - 1) {
                        Thread.sleep(100);
                    }
                } catch (Exception e) {
                    log.error("[Display] SAVE row {}/{} ERROR: deviceSn={}  errorType={}  message={}",
                            i + 1, lines.size(), deviceSn,
                            e.getClass().getName(), e.getMessage(), e);
                    allOk = false;
                }
            }
            log.info("[Display] SAVE_DISPLAY complete: deviceSn={}  allOk={}  rows={}",
                    deviceSn, allOk, lines.size());
            return allOk;
        });
    }

    // ──────────────────── 文本布局转换 ────────────────────

    /**
     * 将平台显示请求转换为行文本列表。
     * <p>
     * 当前仅支持 HORIZONTAL 模式（按 \n 拆分）。
     * VERTICAL 模式依赖真实 LED 点阵参数，暂不实现。
     *
     * @param content   文本内容
     * @param direction 布局方向
     * @return 行文本列表（每行一个字符串）
     * @throws UnsupportedOperationException 当 direction=VERTICAL 时
     */
    private List<String> toLines(String content, DisplayDirection direction) {
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("content must not be null or blank");
        }
        switch (direction) {
            case HORIZONTAL:
                return Arrays.asList(content.split("\n"));
            case VERTICAL:
                throw new UnsupportedOperationException(
                        "VERTICAL display layout is not implemented yet");
            default:
                throw new IllegalArgumentException("Unknown direction: " + direction);
        }
    }

    // ──────────────────── 状态查询 ────────────────────

    /**
     * 查询设备在线状态。
     * <p>
     * 通过心跳缓存判断：最近 30 秒内有心跳视为在线。
     */
    public boolean isDeviceOnline(String deviceSn) {
        return heartbeatRecorder.isDeviceOnline(deviceSn, 30_000L);
    }

    // ──────────────────── 工具方法 ────────────────────

    /**
     * 解码 Base64 字符串，解码失败返回原始值。
     * <p>
     * 臻识协议中部分字段（如 deviceName、license）使用 Base64 编码传输，
     * 目的是避免中文字符在 MQTT 传输中出现编码问题。
     */
    /**
     * 根据臻识相机 FTP 上传路径规则，构造全景图或车牌特写图的对外 HTTP URL。
     * <p>
     * FTP 上传目录结构：{ftpSubdir}/IVS({ip})/channel_0/{type}/{yyyy-MM-dd}/{HHmmss}0000_{plate}.jpg
     * <p>
     * nginx 将该目录映射为对外可访问的 HTTP URL。
     *
     * @param ipAddr    相机内网 IP（从 MQTT 消息 alarmInfo.ipaddr 提取）
     * @param license   车牌号
     * @param eventTs   事件时间戳（秒）
     * @param plateOnly true=车牌特写(plate)，false=全景(full)
     * @return FTP 图片的 HTTP URL，信息不足时返回 null
     */
    private String buildFtpImageUrl(String ipAddr, String license, long eventTs, boolean plateOnly) {
        if (ipAddr == null || ipAddr.isBlank() || license == null || license.isBlank() || eventTs <= 0) {
            return null;
        }
        try {
            java.time.Instant instant = java.time.Instant.ofEpochSecond(eventTs);
            java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneId.of("Asia/Shanghai"));
            String dateStr = zdt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String timeStr = zdt.format(java.time.format.DateTimeFormatter.ofPattern("HHmmss"));
            String typeDir = plateOnly ? "plate" : "full";
            // 文件名格式：{HHmmss}0000_{plate}.jpg（相机 FTP 上行文件名）
            String filename = timeStr + "0000_" + license + ".jpg";
            return String.format("%s/%s/IVS(%s)/channel_0/%s/%s/%s",
                    imagePublicBaseUrl, zhenshiFtpSubdir, ipAddr, typeDir, dateStr, filename);
        } catch (Exception e) {
            log.warn("构造臻识 FTP 图片 URL 失败: ip={}, plate={}", ipAddr, license, e);
            return null;
        }
    }

    private String decodeBase64(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            return "";
        }
        try {
            // 臻识相机固定长度字段可能以 null 字符填充，需先清除
            String cleaned = encoded.replace("\0", "").trim();
            if (cleaned.isEmpty()) {
                return "";
            }
            byte[] decoded = java.util.Base64.getDecoder().decode(cleaned);
            return new String(decoded, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            // 不是 Base64 编码，直接返回原始值（也清除 null 填充）
            return encoded.replace("\0", "").trim();
        }
    }

    @SuppressWarnings("unchecked")
    private Integer extractCarBrand(Map<String, Object> plateResult) {
        if (plateResult == null) {
            return null;
        }
        Map<String, Object> carBrand = (Map<String, Object>) plateResult.get("car_brand");
        return carBrand != null ? (Integer) carBrand.get("brand") : null;
    }

    /**
     * 字节数组转 hex 字符串（空格分隔，每 16 字节换行）。
     * 用于联调时对照协议文档逐字节排查。
     */
    private static String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            sb.append(String.format("%02X ", data[i] & 0xFF));
            if ((i + 1) % 16 == 0 && i < data.length - 1) {
                sb.append("\n");
            }
        }
        return sb.toString().trim();
    }

    private Long toLong(Object value) {
        if (value instanceof Number num) {
            return num.longValue();
        }
        return null;
    }
}
