# Device Access 环境变量配置说明

> **最后更新**：2026-07-25
> **适用版本**：device-access v1.6.0+

---

## 一、关键环境变量

device-access 通过 `/opt/jushan/restart-device-access.sh` 启动，该脚本 `source /opt/jushan/.env.production` 加载全部变量。

| 变量名 | 用途 | 示例值 |
|--------|------|--------|
| `WEBHOOK_URL` | 识别事件推送目标 | `http://127.0.0.1/api/v1/device-webhook/events` |
| `WEBHOOK_SECRET_KEY` | Webhook HMAC-SHA256 签名密钥 | 64 位随机字符串 |
| `API_KEY_1` | 内部 API 鉴权 Key（parking-boot 调用 device-access 时携带） | 32 位随机字符串 |
| `DB_USERNAME` | MySQL 用户名 | `jushan` |
| `DB_PASSWORD` | MySQL 密码 | — |

---

## 二、Webhook URL 注意事项

**必须走 nginx 80 端口**，不能直连 `127.0.0.1:8080`：

- ❌ `http://127.0.0.1:8080/api/v1/device-webhook/events` — 8080 是 Docker 内网端口，宿主机不可达
- ✅ `http://127.0.0.1/api/v1/device-webhook/events` — 经 nginx 反代到 parking-boot

> 此 URL 仅在 device-access 的 `.env.production` 中配置，不在 parking-boot 侧配置。

---

## 三、API Key 配置一致性

device-access 和 parking-boot 使用同一 API Key 做内部调用鉴权：

| 组件 | 配置位置 | Key |
|------|----------|-----|
| parking-boot | Docker 环境变量 `jushan.device-access.api-key` 或 `.env.production` 的 `JUSHAN_DEVICE_ACCESS_API_KEY` | `<见 .env.production>` |
| device-access | `.env.production` 的 `API_KEY_1` | 同上 |

> **不一致会导致所有控闸命令（开闸/关闸/常开/常关）和状态查询失败。** device-access 日志中表现为 `API Key authentication failed`。

---

## 四、芊熠 Q3 图片链路

默认 `alonepush=0`，图片通过 MQTT `result` 消息的 `full_pic`/`plate_pic` 字段以 base64 编码直传：

```
相机 → MQTT result（full_pic/plate_pic base64）
     → QianyiMessageHandler（加 base64: 前缀）
     → PlateRecognizedEventDispatcher.saveQianyiBase64Image()
     → ImageStorageService.saveBytes() 落盘
     → 构造 http://<SERVER_IP>/images/{date}/{sn}/{ts}.jpg
```

无需在相机端额外配置 HTTP 上传。

---

## 五、臻识 C5 图片链路

臻识 C5 通过 FTP 主动上传图片，或通过 MQTT `snapshot` 命令主动抓拍：

```
相机 → FTP 上传到 /opt/jushan/data/device-images/parking/...
     → 或 MQTT snapshot 命令（主动抓拍）
     → 事件中 OSS 签名 URL 由 RemoteImageDownloader 转存本地
```

---

## 六、完整 .env.production 示例

```bash
# 数据库
DB_USERNAME=jushan
DB_PASSWORD=xxxx

# Device Access API Key（必须与 parking-boot 一致）
API_KEY_1=<见 .env.production>

# Webhook 推送（经 nginx 80 端口）
WEBHOOK_URL=http://127.0.0.1/api/v1/device-webhook/events
WEBHOOK_SECRET_KEY=<见 .env.production>

# 白名单同步 API Key
JUSHAN_WHITELIST_SYNC_API_KEY=<见 .env.production>

# parking-boot 用
JUSHAN_DEVICE_ACCESS_API_KEY=<见 .env.production>
DEVICE_ACCESS_WEBHOOK_ALLOWED_IPS=127.0.0.1,172.
```
