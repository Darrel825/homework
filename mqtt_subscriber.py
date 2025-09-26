# mqtt_client/mqtt_subscriber.py
import paho.mqtt.client as mqtt
import json
from datetime import datetime  # 这里不这么导入就用不了datetime
import pymysql
import sys
import os
import traceback  

# 获取项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# 导入 backend.config
from backend.config import Config


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


# 全局缓存
PRODUCTS = {}  #存储商品id，名字，价格
MACHINE_IDS = []
USER_IDS = []   #存储用户支付方式、id、名称


def load_products():
    """加载商品表：product_id -> price"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT product_id, price FROM products")
            result = {row['product_id']: float(row['price']) for row in cursor.fetchall()}
            return result
    finally:
        conn.close()


def load_machine_ids():
    """加载所有 machine_id"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT machine_id FROM machines")
            return [row['machine_id'] for row in cursor.fetchall()]
    finally:
        conn.close()


def load_user_ids():
    """加载所有 user_id"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT user_id FROM users")
            return [row['user_id'] for row in cursor.fetchall()]
    finally:
        conn.close()


def on_connect(client, userdata, flags, rc):
    print("✅ MQTT 已连接")
    client.subscribe("vending/machine/purchase")   #确认了主题（售货机购买行为）


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"📥 收到购买消息: {payload}")

        # 提取消息内容
        user_id = payload['user_id']
        machine_id = payload['machine_id']
        product_id = payload['product_id']
        quantity = int(payload['quantity'])  #记得转化，否则字符串形式无法和库存比较
        total = float(payload['total'])

        # 验证 user_id, machine_id, product_id 是否存在
        if user_id not in USER_IDS:
            print(f"❌ 无效用户 ID: {user_id}")
            return
        if machine_id not in MACHINE_IDS:
            print(f"❌ 无效机器 ID: {machine_id}")
            return
        if product_id not in PRODUCTS:
            print(f"❌ 无效商品 ID: {product_id}")
            return

        # 获取商品单价（用于订单明细）
        unit_price = PRODUCTS[product_id]

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # --- 1. 检查库存（使用 FOR UPDATE 防止并发超卖）---
                cursor.execute("""
                    SELECT volume FROM machine_channels 
                    WHERE machine_id = %s AND product_id = %s FOR UPDATE
                """, (machine_id, product_id))
                result = cursor.fetchone()

                if not result:
                    print(f"❌ 货道未配置: machine_id={machine_id}, product_id={product_id}")
                    return

                current_volume = int(result['volume'])  # ✅ 强制转为整数

                if current_volume < quantity:
                    print(f"❌ 库存不足: 需要 {quantity}, 剩余 {current_volume}")
                    return

                # --- 2. 创建订单 ---
                cursor.execute("""
                    INSERT INTO orders (user_id, machine_id, total, payment_method, status)
                    VALUES (%s, %s, %s, 'wechat', 'completed')
                """, (user_id, machine_id, total))
                order_id = cursor.lastrowid

                # --- 3. 插入订单明细 ---
                cursor.execute("""
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                """, (order_id, product_id, quantity, unit_price))

                # --- 4. 插入支付记录 ---
                transaction_id = f"MQTT_{order_id}_{int(datetime.now().timestamp())}"
                cursor.execute("""
                    INSERT INTO payments (order_id, amount, transaction_id, status)
                    VALUES (%s, %s, %s, 'success')
                """, (order_id, total, transaction_id))

                # --- 5. 扣减库存 ---
                cursor.execute("""
                    UPDATE machine_channels 
                    SET volume = volume - %s, last_restock = NOW()
                    WHERE machine_id = %s AND product_id = %s
                """, (quantity, machine_id, product_id))

                # --- 6. 检查是否售罄，更新货道状态 ---
                if current_volume - quantity == 0:
                    cursor.execute("""
                        UPDATE machine_channels 
                        SET status = 'out_of_stock'
                        WHERE machine_id = %s AND product_id = %s
                    """, (machine_id, product_id))

            # 提交事务
            conn.commit()

            # --- ✅ 美化订单成功提示 ---
            try:
                with conn.cursor() as cursor:
                    # 查询用户名
                    cursor.execute("SELECT username FROM users WHERE user_id = %s", (user_id,))
                    user_result = cursor.fetchone()
                    username = user_result['username'] if user_result else f"用户{user_id}"

                    # 查询机器位置（使用 location_address）
                    cursor.execute("SELECT location_address FROM machines WHERE machine_id = %s", (machine_id,))
                    machine_result = cursor.fetchone()
                    location = machine_result['location_address'] if machine_result else "未知地点"

                    # 查询商品名称
                    cursor.execute("SELECT product_name FROM products WHERE product_id = %s", (product_id,))
                    product_result = cursor.fetchone()
                    product_name = product_result['product_name'] if product_result else f"商品{product_id}"

                # ✅ 打印人性化订单信息
                print(f"✅ 订单创建成功: order_id={order_id}，用户{username}在部署在{location}的{machine_id}号机器购买了“{product_name}”，共支出{total:.2f}元")

            except Exception as e:
                print(f"✅ 订单创建成功: order_id={order_id}，但未能获取详细信息: {e}")
                traceback.print_exc()

        except Exception as e:
            conn.rollback()
            print(f"❌ 订单处理失败: {e}")
            traceback.print_exc()  # ✅ 显示完整堆栈
        finally:
            conn.close()

    except Exception as e:
        print(f"❌ 消息解析失败: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    # 启动时加载数据
    print("🔄 正在从数据库加载商品、用户、机器列表...")
    PRODUCTS = load_products()
    MACHINE_IDS = load_machine_ids()
    USER_IDS = load_user_ids()

    print(f"✅ 商品数: {len(PRODUCTS)}")
    print(f"✅ 机器数: {len(MACHINE_IDS)}")
    print(f"✅ 用户数: {len(USER_IDS)}")

    if not all([PRODUCTS, MACHINE_IDS, USER_IDS]):
        print("❌ 数据加载失败，无法启动 MQTT 订阅者")
        exit(1)

    # 启动 MQTT 客户端
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect("localhost", 1883, 60)
        print("🚀 MQTT 订阅服务启动，等待购买消息...")
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n👋 收到退出信号，监听结束")
    except Exception as e:
        print(f"❌ MQTT 连接失败: {e}")