# backend/config.py
class Config:
    DB_HOST = 'localhost'           # 如果 MySQL 在本地
    DB_USER = 'root'                # 你的 MySQL 用户名
    DB_PASSWORD = 'tqd20030825'   # ⚠️ 替换为你的真实密码
    DB_NAME = 'vending_machine_db'  # 你创建的数据库名
    DB_PORT = 3306