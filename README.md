# 🌦️ Weather Station – Python Container Application

This repository contains the **Python processing layer** of a complete Weather Station system.
The architecture is fully containerized and consists of **three services**:

1. **Python Application** – consumes sensor data over MQTT, stores it in InfluxDB 3 Core, and provides a real-time PyQt interface.
2. **MQTT Broker (Mosquitto)** – communication channel for the ESP32 weather node.
3. **InfluxDB 3 Core** – high-performance time-series database for structured climate data.

The entire development workflow is optimized for **VS Code Dev Containers**, offering a reproducible environment with Python virtualenv, debugging, and GUI support via X11.

---

## 📁 Project Structure

```
.
├── app
│   ├── main.py              # Application entrypoint
│   ├── mqtt_client.py       # MQTT consumer (paho-mqtt)
│   ├── influx_client.py     # InfluxDB 3 Core writer/reader
│   └── __init__.py
│
├── ui
│   ├── main_windown.py      # PyQt6 real-time GUI
│   └── __init__.py
│
├── config
│   ├── config.hpp.in        # Base configuration template
│   ├── mosquitto/           # Broker config, data & logs
│   │   └── config/mosquitto.conf
│   └── influxdb3/           # InfluxDB3 Core local volumes
│       ├── core/data/node0/...
│       └── explorer/...
│
├── requirements.txt          # Python dependencies
└── README.md
```

---

## 🐋 Container Architecture

The system is orchestrated via **docker-compose** and started automatically inside the Dev Container.

**Services**

| Service          | Role                            | Technology        |
| ---------------- | ------------------------------- | ----------------- |
| `app`            | Python data pipeline + PyQt GUI | Python 3.X        |
| `mosquitto`      | MQTT Broker                     | Eclipse Mosquitto |
| `influxdb3-core` | Time-series database            | InfluxDB 3 Core   |

---

## 🧱 Development Environment (Dev Containers)

This project includes first-class support for **VS Code Dev Containers**.

### 🚀 Features

* Python virtual environment automatically created at startup
* All Python tools installed: Pylance, Debugpy, Black, etc.
* X11 forwarding enabled for **PyQt GUI inside container**
* Docker orchestration handled seamlessly by VS Code

### ▶️ Getting Started

1. Open the repository in VS Code.
2. When prompted, **Reopen in Dev Container**.
3. VS Code will:

   * build the image
   * install Python dependencies into `.venv`
   * start the container stack

---

## 🏃 Running the Application

Inside the devcontainer:

### **Run with VS Code Tasks**

* **Run app normally:**

  > `Terminal → Run Task → python: run app.main`

* **Run infrastructure only (MQTT + DB):**

  > `Terminal → Run Task → docker: up infra`

* **Stop containers:**

  > `Terminal → Run Task → docker: down all`

---

## 🐞 Debugging

You can debug both the **Python logic** and the **PyQt GUI**.

### Option A – F5 Debug Session

Automatically starts InfluxDB + Mosquitto before launching your app.

### Option B – Debugpy Attach

Run:

```
python: run app.main (debugpy)
```

Then attach using:

```
Python: Attach using debugpy
```

---

## 🐍 Python Environment

VS Code is configured to automatically activate the virtual environment:

```
.venv/bin/python
```

Inside the devcontainer, no need to manually run:

```
source .venv/bin/activate
```

Unless you are running outside VS Code’s integrated terminal.

---

## 🔌 Data Flow Overview

```
ESP32 → MQTT (mosquitto) → Python (mqtt_client.py)
         ↓                         ↓
    InfluxDB 3 Core ←—— influx_client.py
                          ↓
                      PyQt UI
```

* ESP32 publishes measurements.
* Python subscribes to MQTT topics.
* Records get stored in InfluxDB 3 Core.
* GUI reads the latest values and displays them live.

---

## 📦 Python Dependencies

`requirements.txt` contains:

* **paho-mqtt** – MQTT client
* **influxdb3-python** – InfluxDB 3 client
* **PyQt6** – GUI toolkit
* **numpy / pandas / matplotlib** (optional analytics)

To install manually:

```bash
pip install -r requirements.txt
```

---

## 🧪 Future Extensions

* Real-time charts with PyQtGraph
* Automatic sensor calibration
* Remote OTA updates for ESP32
* Dashboard mode via embedded browser

---

## 📜 License

MIT License – feel free to use and adapt.

---