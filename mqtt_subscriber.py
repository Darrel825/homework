# mqtt_client/mqtt_subscriber.py
import paho.mqtt.client as mqtt
import json
import pymysql
import sys
import os
# 获取项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
# 导入 backend.config
from backend.config import Config

def on_connect(client, userdata, flags, rc):
    print("MQTT Subscriber 连接成功")
    client.subscribe("vending/machine/+/purchase")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload)
        print(f"收到消息: {payload['total_amount']}元")
        save_order_to_db(payload)
    except Exception as e:
        print("消息处理失败:", e)

def save_order_to_db(data):
    conn = pymysql.connect(
        host=Config.DB_HOST,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        database=Config.DB_NAME,
        port=Config.DB_PORT,
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    try:
        # 插入订单
        order_sql = """
        INSERT INTO orders (user_id, machine_id, total, payment_method, status)
        VALUES (%s, %s, %s, %s, 'completed')
        """
        cursor.execute(order_sql, (
            data['user_id'],
            data['machine_id'],
            data['total_amount'],
            data['payment_method']
        ))
        order_id = cursor.lastrowid

        # 插入订单详情
        item = data['items'][0]
        item_sql = """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(item_sql, (
            order_id,
            item['product_id'],
            item['quantity'],
            item['price']
        ))

        conn.commit()  # ✅ 提交事务
        print(f"订单 {order_id} 已存入数据库")

    except Exception as e:
        print("数据库写入失败:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# MQTT 客户端
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)
print("MQTT 订阅者启动，监听购买消息...")
client.loop_forever()