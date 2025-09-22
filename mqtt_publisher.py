# mqtt_client/mqtt_publisher.py
import paho.mqtt.client as mqtt
import json
import time
import random
from datetime import datetime

# MQTT 配置
MQTT_BROKER = "localhost"  # 如果 Mosquitto 在本地
MQTT_PORT = 1883
TOPIC = "vending/machine/V001/purchase"

# 模拟商品
PRODUCTS = [
    {"product_id": 1, "name": "矿泉水", "price": 2.0},
    {"product_id": 2, "name": "可乐", "price": 3.5},
    {"product_id": 3, "name": "薯片", "price": 5.0}
]

def on_connect(client, userdata, flags, rc):
    print(" MQTT Publisher 连接成功")

client = mqtt.Client()
client.on_connect = on_connect

try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    print("售货机模拟器启动，每 5 秒发送一次购买记录...")
    for i in range(5):  # 发送 5 次测试消息
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 2)
        total = product['price'] * qty
        user_id = f"U{random.randint(1000, 9999)}"

        msg = {
            "machine_id": "V001",
            "timestamp": datetime.now().isoformat(),
            "event": "purchase",
            "user_id": user_id,
            "items": [{
                "product_id": product['product_id'],
                "name": product['name'],
                "price": product['price'],
                "quantity": qty
            }],
            "total_amount": total,
            "payment_method": random.choice(['wechat', 'alipay'])
        }

        client.publish(TOPIC, json.dumps(msg))
        print(f"已发送: {product['name']} x{qty} = {total}元")
        time.sleep(5)

    client.loop_stop()
    client.disconnect()
    print("模拟器结束")

except Exception as e:
    print("MQTT 发布失败:", e)