import os
import re
import time
import json
import socket
import threading
import subprocess
from datetime import datetime
import psutil
import paho.mqtt.client as mqtt
from supabase import create_client, Client

# SNMP Configuration
COMMUNITY = "public"
try:
    from pysnmp.hlapi import *
    SNMP_ENABLED = True
except ImportError:
    SNMP_ENABLED = False

# Supabase Configuration
SUPABASE_URL = "https://zskthrdrsryhheitwpaz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpza3RocmRyc3J5aGhlaXR3cGF6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE1MTU0MDIsImV4cCI6MjA2NzA5MTQwMn0.ixvnf6inPwYVF_y9Qs9e8yG0N-9NGnG8o9EFMlUO4yo"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# MQTT Configuration
BROKER = "192.168.43.21"
PORT = 1883
KEEPALIVE = 60
ENV_TOPIC = "datacenter/sensor_data"
INFRA_TOPIC = "datacenter/infrastructure"
NOTIF_TOPIC = "datacenter/notification"
NETWORK_TOPIC = "datacenter/network_status"

# Device Configuration
IOT_DEVICE_IP = "192.168.43.20"
PING_TARGET = "192.168.43.29"
POLL_INTERVAL = 10.0
TRAFFIC_WINDOW = 0.2

# SNMP Functions (if enabled)
if SNMP_ENABLED:
    def snmp_get(oid, target, community=COMMUNITY):
        iterator = getCmd(
            SnmpEngine(),
            CommunityData(community, mpModel=0),
            UdpTransportTarget((target, 161), timeout=1, retries=0),
            ContextData(),
            ObjectType(ObjectIdentity(oid))
        )
        errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
        if errorIndication or errorStatus:
            return None
        return varBinds[0][1]

    def get_snmp_cpu(target):
        val = snmp_get('1.3.6.1.2.1.25.3.3.1.2.1', target)
        return int(val) if val is not None else None

    def get_snmp_memory(target):
        alloc = snmp_get('1.3.6.1.2.1.25.2.3.1.4.1', target)
        size = snmp_get('1.3.6.1.2.1.25.2.3.1.5.1', target)
        used = snmp_get('1.3.6.1.2.1.25.2.3.1.6.1', target)
        if alloc and size and used:
            total_bytes = int(size) * int(alloc)
            used_bytes = int(used) * int(alloc)
            return int((used_bytes / total_bytes) * 100)
        return None

    def get_snmp_disk(target):
        alloc = snmp_get('1.3.6.1.2.1.25.2.3.1.4.2', target)
        size = snmp_get('1.3.6.1.2.1.25.2.3.1.5.2', target)
        used = snmp_get('1.3.6.1.2.1.25.2.3.1.6.2', target)
        if alloc and size and used:
            total_bytes = int(size) * int(alloc)
            used_bytes = int(used) * int(alloc)
            return int((used_bytes / total_bytes) * 100)
        return None

    def get_snmp_net_traffic(target):
        in_oct = snmp_get('1.3.6.1.2.1.31.1.1.1.6.1', target)
        out_oct = snmp_get('1.3.6.1.2.1.31.1.1.1.10.1', target)
        if in_oct is not None and out_oct is not None:
            return (int(in_oct), int(out_oct))
        return (None, None)
else:
    def get_snmp_cpu(target):
        return None
    def get_snmp_memory(target):
        return None
    def get_snmp_disk(target):
        return None
    def get_snmp_net_traffic(target):
        return (None, None)

# Utility Functions
def check_broker_connection(ip, port, timeout=5):
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except OSError:
        return False

def get_ping_stats(target):
    try:
        r = subprocess.run(
            ["ping", "-n", "1", target],
            capture_output=True,
            text=True,
            timeout=5
        )
    except Exception:
        return 0, 100

    avg, loss = 0, 100
    for line in r.stdout.splitlines():
        m_loss = re.search(r"\((\d+)% loss\)", line)
        if m_loss:
            loss = int(m_loss.group(1))
        m_avg = re.search(r"Average\s*=\s*(\d+)ms", line)
        if m_avg:
            avg = int(m_avg.group(1))
    return avg, loss

def get_network_traffic():
    t0 = time.time()
    n0 = psutil.net_io_counters()
    time.sleep(TRAFFIC_WINDOW)
    n1 = psutil.net_io_counters()
    dt = time.time() - t0
    dl = (n1.bytes_recv - n0.bytes_recv) / 1024.0 / dt
    ul = (n1.bytes_sent - n0.bytes_sent) / 1024.0 / dt
    return dl, ul

def collect_stats():
    # CPU Usage
    cpu_pct = get_snmp_cpu(IOT_DEVICE_IP)
    if cpu_pct is None:
        cpu_pct = int(psutil.cpu_percent(None))

    # Memory Usage
    mem_pct = get_snmp_memory(IOT_DEVICE_IP)
    if mem_pct is None:
        mem_pct = int(psutil.virtual_memory().percent)

    # Disk Usage
    disk_pct = get_snmp_disk(IOT_DEVICE_IP)
    if disk_pct is None:
        disk_pct = int(psutil.disk_usage('/').percent)

    # Network Traffic
    dl_ho, ul_ho = get_snmp_net_traffic(IOT_DEVICE_IP)
    if dl_ho is not None and ul_ho is not None:
        dl, ul = dl_ho // 1024, ul_ho // 1024
    else:
        dl_f, ul_f = get_network_traffic()
        dl, ul = int(dl_f), int(ul_f)

    # Network Latency
    latency, loss = get_ping_stats(PING_TARGET)

    # System Uptime
    uptime_s = int(time.time() - psutil.boot_time())
    h, m = uptime_s // 3600, (uptime_s % 3600) // 60
    uptime_h_m = f"{h}h {m}m"
    timestamp = datetime.now().replace(microsecond=0).isoformat()

    return {
        "cpu": f"{cpu_pct}%",
        "memory": f"{mem_pct}%",
        "disk": f"{disk_pct}%",
        "download_kbps": f"{dl}",
        "upload_kbps": f"{ul}",
        "latency_ms": latency,
        "packet_loss": f"{loss}%",
        "uptime_s": uptime_s,
        "uptime_h_m": uptime_h_m,
        "timestamp": timestamp
    }

def safe_percentage_convert(percent_str):
    try:
        return float(percent_str.strip('%'))
    except:
        print(f"⚠ Couldn't convert percentage: {percent_str}")
        return 0.0

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        client.subscribe([
            (ENV_TOPIC, 0),
            (INFRA_TOPIC, 0),
            (NOTIF_TOPIC, 0),
            (NETWORK_TOPIC, 0),
        ])
    else:
        print(f"❌ MQTT connection failed, rc={rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        print(f"⚠ Invalid JSON payload on {topic}: {payload}")
        return

    timestamp = datetime.now().isoformat()
    data["timestamp"] = timestamp

    if topic == ENV_TOPIC:
        print(f"📊 Received sensor data: {data}")
        try:
            supabase.table("sensor_data").insert(data).execute()
            print("✅ Sensor data inserted")

            if "sound" in data and int(data["sound"]) > 4000:
                notif = {
                    "message": f"Sound level high: {data['sound']}",
                    "sensor": "sound",
                    "timestamp": timestamp,
                    "type": "warning",
                    "value": int(data["sound"])
                }
                supabase.table("notifications").insert(notif).execute()

            elif "temperature" in data and float(data["temperature"]) > 35.0:
                notif = {
                    "message": f"Temperature too high: {data['temperature']}°C",
                    "sensor": "temperature",
                    "timestamp": timestamp,
                    "type": "critical",
                    "value": float(data["temperature"])
                }
                supabase.table("notifications").insert(notif).execute()

            elif "humidity" in data and float(data["humidity"]) > 85.0:
                notif = {
                    "message": f"Humidity too high: {data['humidity']}%",
                    "sensor": "humidity",
                    "timestamp": timestamp,
                    "type": "warning",
                    "value": float(data["humidity"])
                }
                supabase.table("notifications").insert(notif).execute()

            elif "gas" in data and int(data["gas"]) > 1500:
                notif = {
                    "message": f"Gas level high: {data['gas']}",
                    "sensor": "gas",
                    "timestamp": timestamp,
                    "type": "warning",
                    "value": int(data["gas"])
                }
                supabase.table("notifications").insert(notif).execute()

            elif "vibration" in data and int(data["vibration"]) > 1:
                notif = {
                    "message": f"Vibration level high: {data['vibration']}",
                    "sensor": "vibration",
                    "timestamp": timestamp,
                    "type": "warning",
                    "value": int(data["vibration"])
                }
                supabase.table("notifications").insert(notif).execute()

            elif "flame" in data and int(data["flame"]) == 1:
                notif = {
                    "message": "🔥 Flame detected! Fire risk!",
                    "sensor": "flame",
                    "timestamp": timestamp,
                    "type": "critical",
                    "value": int(data["flame"])
                }
                supabase.table("notifications").insert(notif).execute()

        except Exception as e:
            print(f"❌ Failed to process ENV data: {e}")

    elif topic == INFRA_TOPIC:
        infra = {
            "cpu": data.get("cpu"),
            "memory": data.get("memory"),
            "disk": data.get("disk"),
            "download_kbps": data.get("download_kbps"),
            "upload_kbps": data.get("upload_kbps"),
            "timestamp": data.get("timestamp")
        }
        print(f"🖥 Received infra metrics: {infra}")
        try:
            supabase.table("infrastructure").insert(infra).execute()
        except Exception as e:
            print(f"❌ Failed to insert infrastructure data: {e}")

    elif topic == NOTIF_TOPIC:
        print(f"🔔 Received notification: {data}")
        try:
            supabase.table("notifications").insert(data).execute()
        except Exception as e:
            print(f"❌ Failed to insert notification: {e}")

    elif topic == NETWORK_TOPIC:
        net_ext = {
            "latency_ms": data.get("latency_ms"),
            "packet_loss": data.get("packet_loss"),
            "uptime_s": data.get("uptime_s"),
            "uptime_h_m": data.get("uptime_h_m"),
            "timestamp": data.get("timestamp")
        }
        print(f"🌐 Received network status: {net_ext}")
        try:
            supabase.table("network_status").upsert(net_ext).execute()
        except Exception as e:
            print(f"❌ Failed to update network status: {e}")

def send_alert(client, sensor, value, alert_type, message):
    try:
        if isinstance(value, str) and '%' in value:
            numeric_value = float(value.strip('%'))
        else:
            numeric_value = float(value)

        alert = {
            "sensor": sensor,
            "value": numeric_value,
            "type": alert_type,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }

        client.publish(NOTIF_TOPIC, json.dumps(alert))
        
        if alert_type in ["critical", "warning"]:
            try:
                supabase.table("system_alerts").insert(alert).execute()
                print(f"✅ Alert saved to system_alerts: {alert}")
            except Exception as e:
                print(f"❌ Failed to save alert to system_alerts: {e}")

    except Exception as e:
        print(f"❌ Alert processing failed: {e}")

def infra_publisher(client):
    while True:
        try:
            stats = collect_stats()
            
            # Publish infrastructure metrics
            infra_pub = {
                "cpu": stats["cpu"],
                "memory": stats["memory"],
                "disk": stats["disk"],
                "download_kbps": stats["download_kbps"],
                "upload_kbps": stats["upload_kbps"],
                "timestamp": stats["timestamp"]
            }
            client.publish(INFRA_TOPIC, json.dumps(infra_pub))

            # Save to network_status table
            net_stat = {
                "latency_ms": int(stats["latency_ms"]),
                "packet_loss": stats["packet_loss"],
                "uptime_s": int(stats["uptime_s"]),
                "uptime_h_m": stats["uptime_h_m"],
                "timestamp": stats["timestamp"]
            }
            try:
                supabase.table("network_status").insert(net_stat).execute()
                print("✅ Saved to network_status")
            except Exception as e:
                print(f"❌ network_status save failed: {e}")

            # Save to netstruct table
            param = "-n" if os.name == "nt" else "-c"
            proc = subprocess.run(
                ["ping", param, "1", IOT_DEVICE_IP],
                capture_output=True, text=True, timeout=5
            )
            dev_status = "Online" if proc.returncode == 0 else "Offline"
            netstruct = {
                "device_ip": IOT_DEVICE_IP,
                "status": dev_status,
                "timestamp": stats["timestamp"]
            }
            try:
                supabase.table("netstruct").insert(netstruct).execute()
                print(f"✅ Saved to netstruct: {dev_status}")
            except Exception as e:
                print(f"❌ netstruct save failed: {e}")

            # System alerts
            cpu_value = safe_percentage_convert(stats["cpu"])
            if cpu_value > 85:
                send_alert(client, "cpuLoad", cpu_value, "critical", f"CPU critical load: {cpu_value}%")
            elif cpu_value > 63.75:
                send_alert(client, "cpuLoad", cpu_value, "warning", f"CPU high load: {cpu_value}%")

            mem_value = safe_percentage_convert(stats["memory"])
            if mem_value > 90:
                send_alert(client, "memoryUsage", mem_value, "critical", f"Memory critical usage: {mem_value}%")
            elif mem_value > 67.5:
                send_alert(client, "memoryUsage", mem_value, "warning", f"Memory high usage: {mem_value}%")

            disk_value = safe_percentage_convert(stats["disk"])
            if disk_value > 90:
                send_alert(client, "diskUsage", disk_value, "critical", f"Disk critical usage: {disk_value}%")
            elif disk_value > 67.5:
                send_alert(client, "diskUsage", disk_value, "warning", f"Disk high usage: {disk_value}%")

            if stats["uptime_s"] < 60:
                send_alert(client, "sysUpTime", stats["uptime_s"], "warning", 
                         f"System recently rebooted: {stats['uptime_h_m']}")

        except Exception as e:
            print(f"⚠ Error in infra_publisher: {e}")
        
        time.sleep(POLL_INTERVAL)

def main():
    print("🚀 Starting MQTT→Supabase bridge...")
    
    if not check_broker_connection(BROKER, PORT):
        print(f"❌ Cannot reach MQTT broker at {BROKER}:{PORT}")
        return

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER, PORT, KEEPALIVE)
        client.loop_start()

        threading.Thread(
            target=infra_publisher, 
            args=(client,), 
            daemon=True
        ).start()

        while True:
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("🛑 Shutting down...")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

if _name_ == "_main_":
    main()