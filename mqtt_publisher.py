# mqtt_client/mqtt_publisher.py
import sys
import os

# 获取当前文件所在目录（mqtt_client）
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录（vending_machine_system）
project_root = os.path.dirname(current_dir)
# 将项目根目录加入模块搜索路径
sys.path.append(project_root)

# 现在可以导入 backend.config
from backend.config import Config

import paho.mqtt.client as mqtt
import json
import random
import time
import pymysql

def get_db_connection():
    return pymysql.connect(
        host=Config.DB_HOST,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        database=Config.DB_NAME,
        port=Config.DB_PORT,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def load_data_from_db():
    """从数据库加载用户、机器、商品信息"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 获取用户 ID 列表
            cursor.execute("SELECT user_id FROM users")
            user_ids = [row['user_id'] for row in cursor.fetchall()]

            # 获取机器 ID 列表
            cursor.execute("SELECT machine_id FROM machines")
            machine_ids = [row['machine_id'] for row in cursor.fetchall()]

            # 获取商品 ID 和价格
            cursor.execute("SELECT product_id, price FROM products")
            products = {row['product_id']: float(row['price']) for row in cursor.fetchall()}

        return user_ids, machine_ids, products
    finally:
        conn.close()

def publish_purchase(client):
    """执行一次购买消息发送"""
    # 发送前重新加载数据
    user_ids, machine_ids, products = load_data_from_db()

    if not all([user_ids, machine_ids, products]):
        print("数据加载失败，跳过本次发送")
        return False

    # 随机选择
    user_id = random.choice(user_ids)
    machine_id = random.choice(machine_ids)
    product_id = random.choice(list(products.keys()))
    quantity = random.randint(1, 3)
    unit_price = products[product_id]
    total = round(unit_price * quantity, 2)

    message = {
        "user_id": user_id,
        "machine_id": machine_id,
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "total": total,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    topic = "vending/machine/purchase"
    result = client.publish(topic, json.dumps(message))
    result.wait_for_publish(timeout=2)

    print(f"发送购买消息: {message}")
    return True

if __name__ == "__main__":
    # 创建 MQTT 客户端
    client = mqtt.Client()
    try:
        client.connect("localhost", 1883, 60)
    except Exception as e:
        print(f"MQTT 连接失败: {e}")
        exit(1)

    print("开始模拟购买行为...")

    # 循环 5 次，每 5 秒发送一次
    for i in range(5):
        print(f"--- 第 {i+1} 次购买 ---")
        success = publish_purchase(client)
        if not success:
            print(f"第 {i+1} 次发送失败")
        time.sleep(5)  # ⏱️ 等待 5 秒

    # 断开连接
    client.disconnect()
    print("模拟结束，已发送 5 条购买消息。")