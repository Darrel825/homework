# backend/app.py
from flask import Flask, render_template, request, redirect, url_for, session, flash, abort
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import check_password_hash
from datetime import datetime
import paho.mqtt.client as mqtt
import json
import time
import pymysql
import sys
import os
import traceback # 打印日志
# 将项目根目录添加到 Python 路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from backend.config import Config

# 获取项目根目录：VENDING_MACHINE_SYSTEM/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 指向 web/templates 和 web/static
TEMPLATE_DIR = os.path.join(BASE_DIR, 'web', 'templates')
STATIC_DIR = os.path.join(BASE_DIR, 'web', 'static')

# 创建 Flask 应用，并指定模板和静态文件目录
app = Flask('vending_manchine_app', template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
app.secret_key = 'your-secret-key-here'  # 安全密钥
app.config.from_object(Config)

# Flask-Login 初始化
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'admin_login'


# 模拟用户类（实际应从数据库加载）
class User(UserMixin):
    def __init__(self, id, role):
        self.id = id
        self.role = role

# 数据库连接函数
def get_db_connection():
    return pymysql.connect(
        host=Config.DB_HOST,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        database=Config.DB_NAME,
        port=Config.DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

# 加载用户回调
@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, role FROM users WHERE user_id = %s", (user_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return User(id=row['user_id'], role=row['role'])
    return None

# 首页 - 用户入口
@app.route('/')
def index():
    return render_template('customer/login.html')

# 用户登录
@app.route('/user/login', methods=['GET', 'POST'])
def user_login():
    if request.method == 'GET':
        return render_template('customer/login.html')

    username = request.form.get('username')
    password = request.form.get('password')

    if not username or not password:
        return "用户名和密码不能为空", 400

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT user_id, role 
                FROM users 
                WHERE username = %s AND password = %s AND role = 'customer'
            """
            cursor.execute(sql, (username, password))
            user = cursor.fetchone()

        if user:
            session['user_id'] = user['user_id']
            session['role'] = user['role']
            return redirect(url_for('customer_index'))
        else:
            return "用户名或密码错误，或您不是客户角色", 401

    except Exception as e:
        print(f"数据库错误: {e}")
        traceback.print_exc()
        return "服务器内部错误", 500

    finally:
        connection.close()

@app.route('/customer')
def customer_index():
    if 'user_id' not in session or session.get('role') != 'customer':
        return redirect(url_for('user_login'))

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 查询所有可用机器（在线或维护中）
        cursor.execute("""
            SELECT machine_id, location_address, machine_status, temperature
            FROM machines 
            WHERE machine_status IN ('online', 'maintenance')
            ORDER BY machine_id
        """)
        machines = cursor.fetchall()

        # 为每台机器加载商品
        for machine in machines:
            machine_id = machine['machine_id']
            cursor.execute("""
                SELECT 
                    p.product_name AS name,
                    p.price AS price,
                    mc.volume,
                    p.product_id
                FROM machine_channels mc
                JOIN products p ON mc.product_id = p.product_id
                WHERE mc.machine_id = %s 
                  AND mc.status = 'active'
                  AND mc.volume > 0
                ORDER BY p.product_name
            """, (machine_id,))
            machine['products'] = cursor.fetchall()

    except Exception as e:
        print(f"❌ 加载数据失败: {e}")
        traceback.print_exc()
        machines = []
    finally:
        conn.close()

    return render_template('customer/index.html', machines=machines)

# 查看单个售货机详情
@app.route('/customer/machine/<int:machine_id>')
def machine_detail(machine_id):  # 移除 @login_required
    if 'user_id' not in session or session.get('role') != 'customer':  
        return redirect(url_for('user_login'))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM machines WHERE id = %s", (machine_id,))
    machine = cursor.fetchone()

    if not machine:
        flash("售货机不存在。")
        return redirect(url_for('customer_index'))

    cursor.execute("""
        SELECT p.name, p.price, i.quantity 
        FROM inventory i 
        JOIN products p ON i.product_id = p.id 
        WHERE i.machine_id = %s
    """, (machine_id,))
    products = cursor.fetchall()

    conn.close()
    return render_template('customer/machine.html', machine=machine, products=products)

# 购买
@app.route('/customer/purchase', methods=['POST'])
def customer_purchase():
    if 'user_id' not in session or session.get('role') != 'customer':
        return redirect(url_for('user_login'))

    user_id = session['user_id']
    items = request.form.getlist('items')
    payment_method = request.form.get('payment_method')

    print("=== 用户购买开始 ===")
    print(f"用户ID: {user_id}")
    print(f"商品列表: {items}")
    print(f"支付方式: {payment_method}")

    if not items or not payment_method:
        flash("请选择商品和支付方式")
        return redirect(url_for('customer_index'))

    conn = None
    try:
        # 建立数据库连接
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. 解析商品并校验 
        selected_products = []
        total_amount = 0.0
        machine_id = None

        for item_str in items:
            try:
                parts = item_str.split('-')
                if len(parts) < 3:
                    flash("商品数据格式错误")
                    return redirect(url_for('customer_index'))

                _machine_id = int(parts[0])
                product_id = int(parts[1])
                unit_price = float(parts[2])

                if machine_id is None:
                    machine_id = _machine_id
                elif machine_id != _machine_id:
                    flash("不能跨售货机购买")
                    return redirect(url_for('customer_index'))

                # 检查库存
                cursor.execute("""
                    SELECT volume FROM machine_channels 
                    WHERE machine_id = %s AND product_id = %s AND status = 'active'
                """, (machine_id, product_id))
                result = cursor.fetchone()

                if not result:
                    flash(f"商品 {product_id} 不存在或未激活")
                    return redirect(url_for('customer_index'))

                stock = int(result['volume']) # 这里需要进行转化，不然会输出空值，致使无法传递
                if stock <= 0:
                    flash(f"商品 {product_id} 库存不足")
                    return redirect(url_for('customer_index'))

                selected_products.append({
                    'product_id': product_id,
                    'unit_price': unit_price
                })
                total_amount += unit_price

            except Exception as e:
                print(f"商品解析异常: {e}")
                flash("商品数据异常")
                return redirect(url_for('customer_index'))

        # 2. 创建订单
        cursor.execute("""
            INSERT INTO orders (user_id, machine_id, order_time, total, payment_method, status)
            VALUES (%s, %s, NOW(), %s, %s, 'completed')
        """, (user_id, machine_id, round(total_amount, 2), payment_method))
        order_id = cursor.lastrowid
        print(f"订单创建成功，order_id = {order_id}")

        # 3. 插入订单明细 
        for prod in selected_products:
            cursor.execute("""
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s)
            """, (order_id, prod['product_id'], 1, prod['unit_price']))
        print(f"已插入 {len(selected_products)} 条订单明细")

        # 4. 扣减库存
        for prod in selected_products:
            cursor.execute("""
                UPDATE machine_channels 
                SET volume = volume - 1, last_restock = NOW()
                WHERE machine_id = %s AND product_id = %s
            """, (machine_id, prod['product_id']))
        print(f"已扣减 {len(selected_products)} 个商品的库存")

        # 5. 提交事务
        conn.commit()
        print("数据库事务已提交！")

        # === 发送 MQTT 消息===
        try:
            client = mqtt.Client()
            client.connect("localhost", 1883, 60)

            mqtt_message = {
                "user_id": user_id,
                "machine_id": machine_id,
                "product_id": selected_products[0]['product_id'],
                "quantity": 1,
                "unit_price": selected_products[0]['unit_price'],
                "total": round(total_amount, 2),
                "payment_method": payment_method,
                "source": "web",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            result = client.publish("vending/user/purchase", json.dumps(mqtt_message))
            result.wait_for_publish(timeout=2)
            client.disconnect()
            print(f"MQTT 消息已发布: {mqtt_message}")

        except Exception as e:
            print(f"MQTT 发布失败（不影响购买）: {e}")
            # 继续执行，不影响订单

        # 最后才返回（确保 commit 已执行）
        flash(f"购买成功！订单号：{order_id}")
        return redirect(url_for('customer_index'))

    except Exception as e:
        print(f"购买过程异常: {e}")
        traceback.print_exc()
        if conn:
            try:
                conn.rollback()
                print("事务已回滚")
            except:
                pass
        flash("购买失败，请稍后重试")
        return redirect(url_for('customer_index'))

    finally:
        if conn:
            conn.close()
            print("🔗 数据库连接已关闭")
    

# 退出
@app.route('/logout')
def logout():
    # 删除 session 中的用户信息
    session.pop('user_id', None)
    session.pop('username', None)
    session.pop('role', None)

    return redirect(url_for('user_login'))

# 管理员登录
@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE username = %s AND password = %s AND role = 'admin'", (username, password))
        admin = cursor.fetchone()
        conn.close()
        if admin:
            login_user(User(id=admin['user_id'], role='admin'))
            return redirect(url_for('admin_dashboard'))
        else:
            flash('管理员登录失败。')

    return render_template('admin/login.html')

# 管理员仪表盘
@app.route('/admin')
@login_required
def admin_dashboard():
    if current_user.role != 'admin':
        return redirect(url_for('customer_index'))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) AS count FROM machines")
    machine_count = cursor.fetchone()['count']

    cursor.execute("SELECT COUNT(*) AS count FROM products")
    product_count = cursor.fetchone()['count']

    cursor.execute("SELECT COUNT(*) AS count FROM orders")
    order_count = cursor.fetchone()['count']

    conn.close()
    return render_template('admin/dashboard.html',
                           machine_count=machine_count,
                           product_count=product_count,
                           order_count=order_count)

# 管理员 - 查看产品列表
@app.route('/admin/products')
@login_required
def admin_products():
    if current_user.role != 'admin':
        return redirect(url_for('customer_index'))

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    products = cursor.fetchall()
    conn.close()
    return render_template('admin/products.html', products=products)

# 管理员 - 添加产品
@app.route('/admin/products/add', methods=['GET', 'POST'])
@login_required
def add_product():
    if current_user.role != 'admin':
        return redirect(url_for('customer_index'))

    if request.method == 'POST':
        name = request.form['name']
        price = request.form['price']
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO products (name, price) VALUES (%s, %s)", (name, price))
            conn.commit()
            flash("产品添加成功。")
        except Exception as e:
            conn.rollback()
            flash("添加失败：" + str(e))
        finally:
            conn.close()
        return redirect(url_for('admin_products'))
    return render_template('admin/add_product.html')

# 管理员 - 编辑产品
@app.route('/admin/products/edit/<int:product_id>', methods=['GET', 'POST'])
@login_required
def edit_product(product_id):
    if current_user.role != 'admin':
        return redirect(url_for('customer_index'))

    conn = get_db_connection()
    cursor = conn.cursor()

    if request.method == 'POST':
        name = request.form['name']
        price = request.form['price']
        try:
            cursor.execute("UPDATE products SET product_name = %s, price = %s WHERE product_id = %s", (name, price, product_id))
            conn.commit()
            flash("产品更新成功。")
        except Exception as e:
            conn.rollback()
            flash("更新失败：" + str(e))
        finally:
            conn.close()
        return redirect(url_for('admin_products'))

    cursor.execute("SELECT * FROM products WHERE product_id = %s", (product_id,))
    product = cursor.fetchone()
    conn.close()
    if not product:
        flash("产品不存在。")
        return redirect(url_for('admin_products'))
    return render_template('admin/edit_product.html', product=product)

# 管理员 - 删除产品
@app.route('/admin/products/delete/<int:product_id>', methods=['POST'])
@login_required
def delete_product(product_id):
    if current_user.role != 'admin':
        return 'Unauthorized', 403

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM products WHERE product_id = %s", (product_id,))
        conn.commit()
        flash("产品删除成功。")
    except Exception as e:
        conn.rollback()
        flash("删除失败：" + str(e))
    finally:
        conn.close()
    return redirect(url_for('admin_products'))

# 管理员 - 订单信息
@app.route('/admin/orders')
@login_required
def admin_orders():
    if current_user.role != 'admin':
        abort(403)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT 
                o.order_id,
                o.user_id,
                o.machine_id,
                o.order_time,
                o.total,
                o.payment_method,
                o.status,
                oi.item_id,
                oi.product_id,
                oi.quantity,
                oi.unit_price,
                p.product_name
            FROM orders o
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN products p ON oi.product_id = p.product_id 
            ORDER BY o.order_time DESC
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            flash("暂无订单数据")
            orders_list = []
        else:
            grouped_orders = {}
            for row in results:
                order_id = row['order_id']
                if order_id not in grouped_orders:
                    grouped_orders[order_id] = {
                        'order': {
                            'order_id': row['order_id'],
                            'user_id': row['user_id'],
                            'machine_id': row['machine_id'],
                            'order_time': row['order_time'],
                            'total': row['total'],
                            'payment_method': row['payment_method'],
                            'status': row['status']
                        },
                        'order_items': []  
                    }
                if row['item_id'] is not None:
                    grouped_orders[order_id]['order_items'].append({
                        'item_id': row['item_id'],
                        'product_id': row['product_id'],
                        'product_name': row['product_name'],
                        'quantity': row['quantity'],
                        'unit_price': row['unit_price']
                    })
            orders_list = list(grouped_orders.values())

    except Exception as e:
        print("❌ 查询失败:", e)
        flash("加载订单失败")
        orders_list = []
    finally:
        conn.close()

    return render_template('admin/orders.html', orders=orders_list)

# 管理员 - 订单详情
@app.route('/admin/orders/<int:order_id>')
@login_required
def order_detail(order_id):
    if current_user.role != 'admin':
        return redirect(url_for('customer_index'))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
    order = cursor

if __name__ == '__main__':
    print(f"项目根目录: {BASE_DIR}")
    print(f"模板目录: {TEMPLATE_DIR}")
    print(f"模板目录是否存在? {os.path.exists(TEMPLATE_DIR)}")
    print(f"登录模板是否存在? {os.path.exists(os.path.join(TEMPLATE_DIR, 'customer', 'login.html'))}")
    app.run(debug=True, host='127.0.0.1', port=5000)